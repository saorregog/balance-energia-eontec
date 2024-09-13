# Balance Energía - EONTEC

## Overview
This project contains an ETL pipeline implementation that brings a solution to the main use case proposed in this EONTEC's course (Introducción a la Ingeniería de Datos). It consists of a backend (built using Python with FastAPI and Socket.IO) and a frontend (built using React).

## Backend

### Technology Stack
- [Backend with FastAPI (HTTP) and Socket.IO (WebSockets)]
- [Data processing using Polars]

### Setup and Run
1. **Navigate to the backend directory**:
    ```sh
    cd backend
    ```

2. **Create the required files**:
   In the the `backend` directory, create `client_secrets.json` (OAuth 2.0 parameters related with accessing Google's APIs) and `settings.yaml` (Allows saving Google access credentials in the current directory) files.

   `client_secrets.json` example:
   ```js
    {
        "web": {
            "client_id": "",
            "project_id": "",
            "auth_uri": "",
            "token_uri": "",
            "auth_provider_x509_cert_url": "",
            "client_secret": "",
            "redirect_uris": [""],
            "javascript_origins": [""]
        }
    }
    ```

    `settings.yaml` example:
    ```yaml
    client_config_backend: settings
    client_config:
        client_id:
        client_secret:

    save_credentials: True
    save_credentials_backend: file
    save_credentials_file: credential_module.json

    get_refresh_token: True

    oauth_scope:
        - https://www.googleapis.com/auth/drive
    ```

3. **Install dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

4. **Configure environment variables**:
    Create a `.env` file in the `backend` directory and fill the following environment variables.

    ```sh
    # Google Drive
    EXTRACTION_FOLDER_ID=
    UPLOAD_FOLDER_ID=

    # FastUpload
    KEY_1=
    KEY_2=
    ```

5. **Run the backend server**:
    ```sh
    uvicorn main:app --reload
    ```

## Frontend

### Technology Stack
- [Frontend with React]

### Setup and Run
1. **Navigate to the frontend directory**:
    ```sh
    cd frontend
    ```

2. **Install dependencies**:
    ```sh
    npm install
    ```

3. **Run the frontend server**:
    ```sh
    npm run dev
    ```

## Additional Information
- Python and Node.js are required to run this project.