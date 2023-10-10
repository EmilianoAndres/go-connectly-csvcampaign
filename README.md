# Implementation for the Batch Campaign csv challenge for the Connectly software engineering interview

## First time? To add the package to your project, run:
`go get github.com/EmilianoAndres/go-connectly-csvcampaign@v0.1.0`

this package exposes 2 structs and a single function.

### BatchSendCampaignRequest
`CsvUrl: the url where the package is going to download the csv from`
`BatchSize: defines the size of the channels for the goroutines`
`Workers: defines the size of the goroutine workerpool`


### BatchSendCampaignResponse
`ApiResponse: a slice containing each individual response from the server. It contains a single id per request.`

### BatchSendCampaign(req *BatchSendCampaignRequest) *BatchSendCampaignResponse

this method contains the whole business logic