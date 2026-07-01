Open Book Format (~60 min total)
The goal of this interview project is to get you thinking and explore ideas together. Please use any source and any AI tools to help, but be ready to discuss and defend your ideas at a technical level, and work live to iterate and expand them.
There are no “gotcha” parts but there is real-world complexity to be aware of. Please ask questions to clarify anything. Feel free to make any assumptions or generalizations that make the problem easier, just be clear about why you’re making them and what effect they have.
The goal is not to answer every single question below; this is not a traditional test with right or wrong answers. The idea is to use this as a launching board for in depth technical discussion about cloud architecture and data modeling.
<aside> ⚠️ Please be prepared to share your screen during the call.
</aside>
Action Items prior to interview + Recommended tools

* This document contains one real world business challenge, including multiple sections like Architecture, Scalability, and Data Validation.
* Have something prepared for the interview oriented around the subject matter and example questions. This should take 2 hours or less. You can express your thoughts however you like, i.e. diagrams, written paragraphs, code etc.
* Your three separate interviews will focus on their assigned competency areas.
* Please be prepared to share your screen
   * We recommend Miro / Lucid if you prefer a whiteboard style – https://miro.com/
   * We recommend Notion or Google Docs if you prefer a document based style
   * You can use your own local text editor or own IDE
Architecture (~30 min)
We read in financial data from many partners. Some of the data is real-time and queried when it is needed and other data is read in on a recurring basis. Assume we have a system that needs to query a data partner for our clients’ account details and land that data in our data platform (raw object store, normalized relational tables, and a query-ready data lake).
The flow is as following

1. At 7am Farther calls an API endpoint to request all clients and their details. Only 1 request is sent per day. This returns a request id.
2. Farther then polls a status endpoint to see when the request id is ready.
3. When the request id is ready, Farther hits an additional endpoint to retrieve the dataset.
4. Farther will download the zip file from the provided URL.
5. Farther unzips the zip file and extracts all of the JSON files, again 1 file per client.
6. Farther parses and validates each file and inserts data into the selective database tables. The raw JSON is also retained in object storage and a normalized Parquet copy is written to the data lake for analytical consumers.
Assumptions

* Farther has 1,000 clients
* Clients on average have 2 brokerage accounts
* Each account has 10–100 holdings and produces a handful of transactions per day
<aside> 📊 Sequence diagram (same as the original Backend Technical Discussion): Farther → Partner: Request Client Accounts Partner → Farther: Ack Partner → Farther: Webhook (zip URL) Farther → Partner: Ack Farther → Partner: GET ZIP Partner → Farther: Zip file Farther: Unzip file Farther: Process each client's JSON file Farther: Write client data to database
</aside>
Authentication (OAuth 2.0)
The partner authenticates with OAuth 2.0. Wire this up before designing the data path — auth failures are the most common cause of partner-integration regressions and the slowest to debug.
Requirements

1. Outbound (Farther → partner): OAuth 2.0 client credentials grant. Farther exchanges a `client_id` / `client_secret` for a short-lived access token (typically 15–60 min TTL) and presents it as a Bearer token on every REST call to the partner.
Questions

1. Write a high level overview of this service. Mock out all of the implementation details and just focus on the high-level flow of the application.
2. What modules would you break this application into? What is the responsibility of each module? Which run synchronously, which run asynchronously, and where are the queue / storage boundaries?
3. Where will this architecture scale to? What limitations? Where does it break first — the API call, the download, the unzip, the parser, the writer?
4. What is your estimate to write this service? Where are the unknown-unknowns? What parts have the highest complexity and what parts have the lowest complexity?
5. Where does each piece run in AWS? Lambda, ECS / Fargate, Glue, Step Functions — defend your choice. At what data volume does that choice change?
6. How do you know a given day’s load is complete — expected file inventory, control totals, market-value reconciliation against the prior day? What is the SLO you would set on this pipeline and why?
7. Authentication:
   1. Token cache: How will we store and update short lived tokens?
   2. where will client id/ client secret be stored and why?
   3. Observability: How will we monitor for 401 / 403 / 5xx errors?
   4. how can we narrow the scope the partner exposes (e.g. read-positions, read-transactions)
Evolving the service
Scaling the number of clients
After a year Farther has grown to 10,000 clients. A year after that Farther has grown to 1,000,000 clients. More than 1 client can come in each file.

* What changes would you make in your architecture?
* At what point does the single zip-file pattern break entirely? What replaces it — partitioned object prefixes, a manifest, a streaming feed?
* How does your data lake layout (partitioning by as_of_date / client / account) hold up at 1M clients?
Move to a modern event-driven AWS architecture
Farther receives complaints that having stale data is causing client frustration. We talk our partner into emitting events the moment a client’s data is ready. We want to take this opportunity to redesign the pipeline as a modern AWS-native, event-driven system — no scheduled cron, no monolithic worker, every step retryable in isolation, and every component pageable when it falls behind.
Can you design a new flow, mapped to AWS services:
Questions

1. Why each AWS service? Defend your choices for edge auth, event routing, buffering, retries and error handling, and compute. At what data volume does any of these break?
2. How do you guarantee no client is double-processed? Where does the dedupe key live?
3. How do you observe this system end-to-end? What metrics make "the pipeline is healthy" vs. "we are falling behind" obvious? What is your alarm strategy on the DLQ?
4. After a bad deploy, yesterday’s events all failed. How do you replay them without breaking idempotency?
5. AWS has a regional outage. How can we determine the blast radius and remediation
6. How does this event-driven shape change your data lake partitioning and ordering guarantees vs. the daily cron design?
Data design (~30 min)
Each client has the JSON information below.

```json
{
  "id": "c_1234",
  "name": "John Adams",
  "accounts": [{
    "id": "a_1234",
    "value": "1208",
    "currency": "USD",
    "name": "Brokerage",
    "type": "Brokerage"
  }, {
    "id": "a_2345",
    "value": "1045",
    "currency": "CAD",
    "name": "John's Retirement",
    "type": "IRA"
  }],
  "holdings": [{
    "id": "h_1234",
    "accountId": "a_1234",
    "name": "Apple Inc",
    "security": "AAPL",
    "quantity": 14.5,
    "buyPrice": 145,
    "isCashLike": false
  }, {
    "id": "h_2345",
    "accountId": "a_1234",
    "name": "Apple Bond 2026 6%",
    "security": null,
    "quantity": 140,
    "buyPrice": 0.98,
    "isCashLike": false
  }],
  "transactions": [{
    "id": "t_1234",
    "accountId": "a_1234",
    "holdingId": "h_1234",
    "type": "SELL",
    "quantity": 2,
    "value": 167,
    "date": "2024-04-13",
    "settleDate": "2024-04-15"
  }]
}

```

Questions

1. Where would you store this data? Do different use cases demand different types of storage? Consider:
   1. API use cases (querying holdings by account id or client_name). If using a database, what schema would you use? what indices?
   2. MCPs and Agents - what considerations do we need to optimize for agentic workflows and autodiscovery?
   3. Discoverability and Documentation - how can we make it easy for business users and agent to discover and understand the data?
   4. Schema evolution - Farther is rapidly growing and expanding our vendors and even types of vendors. Think about schema evolution and how that affects your answers above.
Data validation (~30 min)
At Farther we believe that strong types, explicit schemas, and validation at the boundary.
What tools would you use to do data validation?
Create an architectural diagram of a validation layer for the data, and implement validators for 1 or 2 types of bad data (holdings without a security identifier, taxlots with negative quantities, or others you can think of)