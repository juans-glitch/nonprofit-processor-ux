# Nonprofit 990 Data Processor

A simple web-based tool for extracting financial data from nonprofit tax filings (Form 990). The tool takes a CSV file containing a list of nonprofit Employer Identification Numbers (EINs) and tax years, scrapes the necessary data from ProPublica's Nonprofit Explorer, and returns a single, consolidated CSV file to the user.

## Architecture 🏗️

The project is composed of two main parts that work together:

* **Frontend**: A static `index.html` file containing HTML, CSS, and JavaScript. It provides the user interface for file uploads and displays status messages. It is designed to be hosted on any static site host, such as GitHub Pages.
* **Backend**: A Python-based **Google Cloud Function**, deployed on **Cloud Run**. This serverless function contains all the logic for parsing the input CSV, scraping ProPublica, processing XML tax forms, and generating the final CSV output.

## Features ✨

* **Simple CSV Upload**: An intuitive drag-and-drop or file-select interface for uploading the input file.
* **Parallel Processing**: The backend uses multithreading to process up to 10 filings concurrently, dramatically speeding up the processing time for large lists.
* **Robust Input Validation**: The backend checks for common errors in the input file, such as exceeding a maximum row count or missing required columns (`ein`, `year`), and provides clear feedback.
* **User-Friendly Error Handling**: The frontend displays specific, easy-to-understand error messages to help users correct their input.
* **Dynamic Filenames**: Downloaded CSV files are automatically named with the current date (e.g., `nonprofit_data_extract_2025-09-01.csv`) to help with organization.

## Setup & Configuration 🛠️

Follow these steps to deploy and configure your own instance of this tool.

### Prerequisites

* A Google Cloud Project with billing enabled.
* A GitHub account.
* `gcloud` CLI (optional, for command-line deployment).

### 1. Backend Deployment (Google Cloud Run)

The backend is deployed as a single Cloud Function on Cloud Run, built directly from the source repository.

1.  **Clone the Repository**: Clone this repository to your local machine or your GitHub account.
2.  **Create a Cloud Run Service**:
    * In the Google Cloud Console, navigate to **Cloud Run**.
    * Click **"Create Service"**.
    * Select **"Continuously deploy new revisions from a source repository"** and connect it to your GitHub repo.
    * In the **Build Configuration** step, use the following settings:
        * **Build Type**: Google Cloud's buildpacks
        * **Entrypoint**: (leave blank)
        * **Function target**: `process_ein_list`
    * Deploy the service.
3.  **Get the Service URL**: Once deployed, Cloud Run will provide a public URL for your service. Copy this URL.

### 2. Frontend Configuration

The frontend needs to know the URL of the backend to send requests to.

1.  **Edit `index.html`**: Open the `index.html` file.
2.  **Update the URL**: Find the following line in the `<script>` section:
    ```javascript
    const CLOUD_FUNCTION_URL = 'PASTE_YOUR_TRIGGER_URL_HERE';
    ```
3.  Replace `PASTE_YOUR_TRIGGER_URL_HERE` with the URL you copied from your Cloud Run service.
4.  **Host the Frontend**: Commit this change. You can host this `index.html` file anywhere, including:
    * **GitHub Pages**: The easiest option. Enable it in your repository's **Settings > Pages**.
    * **Locally**: Simply open the `index.html` file in your web browser.

## Project Structure 📁

```
.
├── main.py        # The Google Cloud Function backend logic
├── requirements.txt # Python dependencies for the backend
└── index.html       # The static HTML/CSS/JS frontend
```

## Future Enhancements 🚀

This tool can be extended with additional features for security, usability, and robustness.

* **Security**: Implement a login system (e.g., Basic Authentication with credentials stored in Secret Manager) to restrict access.
* **Cost Control**: Create a "circuit breaker" function that automatically disables the service if it approaches a Cloud Billing budget limit.
* **Advanced UX**:
    * Add a real-time progress bar by having the backend report its status to Firestore.
    * Implement email notifications for long-running jobs.
* **Caching**: Use Memorystore or Firestore to cache results for frequently requested EIN/year pairs to improve speed and reduce external requests.
* **Monitoring**: Integrate structured logging with Cloud Logging and create dashboards and alerts in Cloud Monitoring to track performance and error rates.
