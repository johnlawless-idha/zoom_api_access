# Zoom API as a Data Resource
 - Note: For a description of data features, see [the data dictionary](https://github.com/johnlawless-idha/zoom_api_access/blob/main/dictionary.md)

## Table of Contents
1. [Overview](#overview)
2. [Limitations](#rate_limits)

## Introduction
 - This project utilizes [Zoom API](https://marketplace.zoom.us/docs/api-reference/zoom-api/) requests to scrape quality and user data from virtual visits. Visit this documentation page for information of API use, including policies and rate limits.

 - As the data acquisition was acquired using Python, the wrapper [zoomus](https://github.com/prschmid/zoomus) is also employed to ease the process of making requests to the API. This wrapper generates an object that will automatically authenticate each request when utilizing a method to the defined object in python code. Documentation can be found in the above link. 

## Overview <a name="overview"></a>

### Authentication Method
 - The Zoom API requires admin authentication to securely make requests. Testing and initial data collection was accomplished using [JWT (Json Web Tokens)](https://marketplace.zoom.us/docs/guides/auth/jwt) as an authentication method. The admin for BCH's outpatient zoom account created a JWT app in the marketplace following [these instructons](https://marketplace.zoom.us/docs/guides/build/jwt-app/) to generate an API secret and key with set expiration dates, which allow for authenticated requests. This method, however, is only intended for testing and initial work, and a more long term solution is necessary.
 - Going forward, authentication will most likely need to be set up using [OAuth](https://marketplace.zoom.us/docs/api-reference/using-zoom-apis), which ultimately can similarly be set up as an app in the zoom marketplace. Documentation to set up an app can be [found here](https://marketplace.zoom.us/docs/guides/build/oauth-app/), however OAuth will require a redirect URL for authentication. 

### Method of Data Extraction
- The zoom API has multiple scopes of use, ranging from user metadata to billing information, and is primarily designed for the automation of meeting scheduling and maintenance. This project primarily makes use of 2 scopes in x general steps, as shown below. Requests made by the API are returned as json objects, which are transformed via python code into relational data tables, as the primary goal of this data was for data analysts to quickly aggregate and analyze the data.

1) The first scope is users, by which a [list of all users](https://marketplace.zoom.us/docs/api-reference/zoom-api/methods/#operation/userCreate) on the zoom account are returned. Each user has a unique identifier called "id," which references a user. 
2) All remaining steps utilize the dashboard scope. For each unique id'd user in the zoom account, a request is made to [list all meetings](https://marketplace.zoom.us/docs/api-reference/zoom-api/methods/#tag/Dashboards) under that user over a given time frame (maximum is a 1 month period per request, but can go back a total of 6 months into the past). This forms the basis of final data table, where each row has the unique identifier of the zoom meeting's id.
3) For each unique meeting id, the dashboard request [list meeting participants](https://marketplace.zoom.us/docs/api-reference/zoom-api/methods/#operation/dashboardMeetingDetail) is made, which returns all participants in a meeting, as well as devices used, connection type, and other data points. This data is appended to the original dataset.
4) Finally, for each meeting id, the request [list meeting participant Qos](https://marketplace.zoom.us/docs/api-reference/zoom-api/methods/#operation/dashboardMeetingParticipantsQOS) (quality of service) is made, which returns a large json object as a response, detailing multiple quality metrics for audio, video, and screen sharing quality for every minute of the zoom meeting. Before assembling this into a data table, the data is aggregated into the overall meeting's minimum, average, and maximum metrics. 
5) From this response, the number of minutes that any given metric falls far outside [zoom's recommended metric values](https://support.zoom.us/hc/en-us/articles/202920719-Accessing-meeting-and-phone-statistics) are tallied, and a proportion of the meeting that shows unnaceptable metric values are calculated, so as to flag meetings with audio, video, or screen share quality issues. All of this data is appended to the final data table.  

## Rate Limits
- Zoom sets [rate limits](https://marketplace.zoom.us/docs/api-reference/rate-limits/) on API requests, both per second and daily. Dashboard requests, which make up most of the requests for data extraction, are classified as heavy rate limit APIs, which are limited to 10 requests/second, and a maximum of 30,000 requests/ day. This daily limit is shared across all sub accounts, and resets at the Zoom server reset time of 7 PM. **There is another department that also uses this API for EPIC integration,** so we should attempt to stay far away from this daily limit whenever possible to avoid service impact to their team. 
- Each request is not necesarily a single request either. JSON responses are separated by [pagination](https://marketplace.zoom.us/docs/api-reference/pagination/), with longer responses containing a "next page token" that requires another request to complete the compilation of data. This is flexible and difficult to predict, but larger participant counts and longer durations can lead to multiple requests. At this time, keeping the limit of zoom meetings to convert to under 6000 per day seems to not cause the API to hit this rate limit. 