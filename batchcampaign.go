package connectlybatchcampaigncsv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

// Assuming we only use templated messages
type connectlyTemplateMessageDTO struct {
	Sender             string                                  `json:"sender"`
	Number             string                                  `json:"number"`
	Language           string                                  `json:"language"`
	TemplateName       string                                  `json:"templateName"`
	TemplateParameters []connectlyTemplateMessageParametersDTO `json:"parameters"`
	CampaignName       string                                  `json:"campaignName"`
}

type connectlyTemplateMessageParametersDTO struct {
	Name     string  `json:"name"`
	Value    *string `json:"value"`
	FileName *string `json:"filename"`
}

type apiResponse struct {
	Id string `json:"id"`
}

type BatchSendCampaignRequest struct {
	CsvUrl    string
	BatchSize int
	Workers   int
}

type BatchSendCampaignResponse struct {
	ApiResponses []apiResponse
}

// This function takes an object containing a url to download a csv, and a set batch and workerpool size for GoRoutines configuration.
// It then parses the csv for known columns and assigns values, and sends batches of api requests.
// Returns an object with a collection of "id" responses.
func BatchSendCampaign(req *BatchSendCampaignRequest) *BatchSendCampaignResponse {

	// Change this for the real API implementation. Using mock server now.
	connectlyUrl := "https://cde176f9-7913-4af7-b352-75e26f94fbe3.mock.pstmn.io/v1/businesses/f1980bf7-c7d6-40ec-b665-dbe13620bffa/send/whatsapp_templated_messages"

	// instance an http client and context
	client := &http.Client{}
	ctx := context.Background()

	// Business logic. Workers and Batchsize are set by the user.
	resp, err := fetchCSVAndSendAPIRequests(ctx, client, req.Workers, req.CsvUrl, req.BatchSize, connectlyUrl)
	if err != nil {
		return &BatchSendCampaignResponse{}
	}

	// Parse responses.
	for i, element := range resp.ApiResponses {
		print(fmt.Sprintf("response number %d was: %s \n", i+1, element.Id))
	}

	fmt.Println("process finished successfully")
	return &resp
}

func fetchAndParseCsv(urlStr string, client *http.Client) ([]connectlyTemplateMessageDTO, error) {

	// we use url.Parse to check if it's an http link or a path to a local file
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var csvReader *csv.Reader
	if strings.HasPrefix(parsedURL.Scheme, "http") {
		// fetch csv
		response, err := client.Get(urlStr)
		if err != nil {
			return nil, err
		}

		response.Body.Close()

		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("request failed. Status: %s", response.Status)
		}

		csvReader = csv.NewReader(response.Body)
	} else {
		// locally read csv
		file, err := os.Open(parsedURL.Path)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		csvReader = csv.NewReader(file)
	}

	columns, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	var messages []connectlyTemplateMessageDTO

	// Template name
	var templateName string

	// Parameters
	var params []connectlyTemplateMessageParametersDTO

	// separate template from params, add all the template types to a slice
	for _, element := range columns {
		if parts := strings.Split(element, ":"); len(parts) == 2 {
			if parts[1] == "filename" {
				continue
			}
			if templateName == "" {
				templateName = parts[0]
			}
			if parts[0] == templateName {
				params = append(params, connectlyTemplateMessageParametersDTO{Name: parts[1]})
			}
		}
	}

	// Iterate through rows and process their values based on the columns
	for {
		row, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		// create a new slice for each message. This prevents the original slice
		// from being modified, as slices in Go are referenced by default.
		// make memallocs new space in memory for a new copy.
		newParams := make([]connectlyTemplateMessageParametersDTO, len(params))
		copy(newParams, params)

		// each row is a new message
		message := connectlyTemplateMessageDTO{
			TemplateName:       templateName,
			TemplateParameters: newParams,
		}

		for i, value := range row {
			columnName := columns[i]
			processColumnValue(&message, columnName, value)
		}
		messages = append(messages, message)
	}

	return messages, nil
}

func processColumnValue(message *connectlyTemplateMessageDTO, columnName, value string) {
	// directly assign a value if it is a known column. Proceed to parse otherwise
	switch columnName {
	case "sender":
		message.Sender = value
	case "number":
		message.Number = value
	case "language":
		message.Language = value
	case "campaign_name":
		message.CampaignName = value
	default:
		handleDynamicColumns(message, columnName, value)
	}
}

func handleDynamicColumns(message *connectlyTemplateMessageDTO, columnName, value string) {
	// because of the format key:value inside the columns, we proceed to separate the particular
	// case where we got a header_document and, by consequence, a filename.
	// otherwise just assign the value to the parameter
	parts := strings.Split(columnName, ":")
	if len(parts) != 2 {
		return
	}

	switch parts[1] {
	case "filename":
		for i := range message.TemplateParameters {
			if message.TemplateParameters[i].Name == "header_document" {
				if value != "" {
					message.TemplateParameters[i].FileName = &value
				}
				return
			}
		}
	default:
		for i := range message.TemplateParameters {
			if message.TemplateParameters[i].Name == parts[1] {
				if value != "" {
					message.TemplateParameters[i].Value = &value

				}
				return
			}
		}
	}
}

func fetchCSVAndSendAPIRequests(ctx context.Context, httpClient *http.Client, workerNum int, csvURL string, batchSize int, apiUrl string) (BatchSendCampaignResponse, error) {
	// Fetch and Parse csv
	messages, _ := fetchAndParseCsv(csvURL, httpClient)

	messagesChan := make(chan connectlyTemplateMessageDTO, batchSize)
	responseChan := make(chan apiResponse, batchSize)
	var wg sync.WaitGroup

	go func() {
		for _, message := range messages {
			messagesChan <- message
		}
		close(messagesChan)
	}()

	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// marshal every struct into json and send an api request
			for message := range messagesChan {
				jsonData, err := json.Marshal(message)
				if err != nil {
					fmt.Println("could not encode json: ", err)
					continue
				}
				resp, err := sendAPIRequest(ctx, httpClient, apiUrl, jsonData)
				if err != nil {
					fmt.Printf("Error sending message: %v\n", err)
				} else {
					responseChan <- resp // Store the response
				}
			}
		}()
	}

	// waiting for all the workers to be done, then closing the response channel
	go func() {
		wg.Wait()
		close(responseChan)
	}()

	// collecting every response
	var batchResponse BatchSendCampaignResponse
	for response := range responseChan {
		batchResponse.ApiResponses = append(batchResponse.ApiResponses, response)
	}

	return batchResponse, nil
}

// sendAPIRequest sends an API request and returns the response as a string.
func sendAPIRequest(ctx context.Context, client *http.Client, url string, jsonData []byte) (apiResponse, error) {
	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		return apiResponse{}, err
	}

	// Set headers for testAPI. This should go to a config file, or to a local storage.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-Key", "<API Key>")
	req.Header.Set("x-mock-response-code", "201")

	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return apiResponse{}, fmt.Errorf("request failed. Status: %s", resp.Status)
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiResponse{}, err
	}

	var apiResp apiResponse

	json.Unmarshal(responseBody, &apiResp)

	return apiResp, nil
}
