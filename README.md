
# **Project: Large Dataset Processing API**

## Goal:
Build a FastAPI-based web service that processes large datasets using **Polars** and **Pandas**. 
The service should:
- Load, clean, and aggregate data.
- Expose a FastAPI endpoint to return processed results.
- Write results to a JSON or Parquet file.

You will compare **Polars** and **Pandas** performance, handle missing values, 
and test the API via the interactive `/docs` interface.

## **Project Structure**

```
data_api/
    ├── .gitignore
    ├── .env.example
    ├── README.md
    ├── main.py
    ├── pyproject.toml
    ├── poetry.lock
    ├── processor/
    │   ├── __init__.py
    │   ├── load_data.py
    │   ├── clean.py
    │   ├── aggregate.py
    └── tests/
        ├── __init__.py
        └── test_processor.py
```

### **Features:**

1. **FastAPI Endpoint**: A GET endpoint that loads, cleans, and aggregates dataset information. 
2. **Dependency Management**: Use Poetry for package management.
3. **Virtual Environment**: Use `venv` for isolated Python environments.
4. **Data Processing**: Efficiently handle large datasets with **Polars** and **Pandas**.
5. **Data Cleaning**: Handle missing values, outliers, and incorrect formatting.
6. **File Handling**: Save processed results in JSON and Parquet formats.
7. **API Documentation**: Use Swagger UI (`/docs`) for interactive testing.

## **Tasks**

### **Task 1: Set Up Environment**

1. Initialize a Python project using **Poetry**.
2. Set up a virtual environment (`venv`) and install **FastAPI, Polars, Pandas**.
3. Create a FastAPI boilerplate with a simple root endpoint (`/`).

### **Task 2: Data Loading**

1. Create a script to load any large dataset (CSV format, minimum of 100000 rows).
2. Implement functions to read datasets using both **Pandas** and **Polars**.
3. Compare loading times and document findings.

### **Task 3: Data Cleaning**

1. Introduce missing values into a dataset.
2. Implement functions to handle missing values (e.g., fill with mean, drop rows).
3. Apply cleaning transformations using **Pandas** and **Polars**.
4. Document which method is more efficient and why.

### **Task 4: Data Aggregation**

1. Group dataset by a categorical column (e.g., `category`).
2. Compute **mean, sum, count** for a numeric column.
3. Implement aggregation using both **Pandas** and **Polars**.
4. Expose results via a FastAPI endpoint (`/aggregate`).

### **Task 5: API Integration**

1. Create a FastAPI service with a `/process-data` endpoint.
2. This endpoint should:
   - Load dataset
   - Clean and aggregate data
   - Return processed results in JSON format.
3. Implement error handling for incorrect input formats.

### **Task 6: Saving and Retrieving Processed Data**

1. Save processed data in **JSON** and **Parquet** formats.
2. Implement API endpoints to download these files (`/download-json`, `/download-parquet`).

## **Deadline**
- The project should be submitted by **Friday, February 28th, 2025, 11:59 PM**.

## **Submission**

- Share your **PR** in the `task-submission` channel and tag your mentors.
- GitHub Repository: https://github.com/Data-Epic/data-wrangling/

## **Rubrics**

- **Completeness**: All parts of the task are implemented and working.
- **Code Quality**: Code is clean, well-documented, and follows best practices.
- **Testing**: Unit tests are comprehensive and cover edge cases.
- **Git Usage**: Proper use of Git for version control, with meaningful commit messages.

Good luck, and happy coding! 🚀
