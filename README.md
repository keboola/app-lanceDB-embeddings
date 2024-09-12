# Embedding Transformation

This component allows you to embed tables using OpenAI embedding algorithms with data provided from your KBC project.

- [TOC]

---

## Configuration

### Parameters:

#### AI Service Provider: OpenAI

- **API Key (`#api_token`):** Obtain your API key from the [OpenAI platform settings](https://platform.openai.com/account/api-keys).

### Other options:
- **Column to Embed (`embed_column`)**: Specify the column that contains the text data to be embedded.
- **Embedding Model (`model`):** The model that will generate the embeddings. Choose from:
  - `text-embedding-3-small`
  - `text-embedding-3-large`
  - `text-embedding-ada-002` [Learn more](https://platform.openai.com/docs/models/embeddings).
- **Output Format (`output_format`):** Determines if embeddings will be sent to a zipped Lance file or to a Keboola Table (CSV).
- **Incremental Load (`incremental load`):** If enabled, the table will update instead of being overwritten.
- **Output Table Name (`output_table_name`)**
- **Primary Keys (`primary_keys`):**
---

### Component Configuration Example

**Generic configuration**

```json
{
  "#apiKey": "your-openai-api-key",
  "model": "ada_002",
  "embedColumn": "description",
  "outputFormat": "csv"
}
```

This configuration uses the `ada_002` model to embed the `description` column and outputs the result in CSV format.

**Row configuration**

```json
{
  "embedColumn": "title",
  "destination": {
    "output_table_name": "embedded_table",
    "incremental_load": true,
    "primary_keys": "id"
  }
}
```

In this example, the embedding column is set to `title`, and the results are stored in the `embedded_table` with incremental loading enabled. The primary key is the `id` column.

---

# Development

If required, change the local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in the `docker-compose.yml` file:

```
volumes:
  - ./:/code
  - ./CUSTOM_FOLDER:/data
```

Clone this repository, initialize the workspace, and run the component with the following command:

```
git clone git@github.com:keboola/app-transformation-lanceDB-embeddings.git
cd app-transformation-lanceDB-embeddings
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

---

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/).
