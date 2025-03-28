# **Music Streaming Analytics using PySpark Structured API**
This project uses PySpark and Spark Structured APIs to analyze large-scale music streaming data and extract actionable insights. The data consists of user listening behavior, including song plays, genres, and moods, collected from a fictional music streaming platform. By leveraging PySpark, the project focuses on performing data transformations, aggregations, and analyses to uncover trends in user engagement, genre preferences, and listening patterns.

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:
assignment-3-spark-structured-api-VinayD2028/ <br />
  ├── outputs/ <br />
  │ ├── avg_listen_time_per_song <br />
  │ ├── genre_loyalty_scores <br />
  │ ├── happy_recommendations <br />
  │ ├── night_owl_users <br />
  │ ├── top_songs_this_week <br />
  │ └── user_favorite_genres <br />
  ├── datagen.py <br />
  ├── listening_logs.csv <br />
  ├── main.py <br />
  ├── Readme.md <br />
  ├── Requirements.txt <br />
  └── songs_metadata.csv <br />


- **outputs/**: Contains the results of each task.
- **datagen.py**: Script used to generate sample data for listening logs.
- **listening_logs.csv**: Input file containing user listening data.
- **main.py**: Main script performing the analysis.
- **Requirements.txt**: Contains Python dependencies.
- **songs_metadata.csv**: Metadata for songs, including genres and moods.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd assignment-3-spark-structured-api-VinayD2028/
   ```
2. **Execute the Main Script**:
   ```bash
   spark-submit main.py
   ```
3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

#### **b. Running with Docker (Optional)**

1. **Start the Spark Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Access the Spark Master Container**:
   ```bash
   docker exec -it spark-master bash
   ```

3. **Navigate to the Spark Directory**:
   ```bash
   cd /opt/bitnami/spark/
   ```

4. **Run Your PySpark Scripts Using `spark-submit`**:
   ```bash
   
   spark-submit main.py
   ```

5. **Exit the Container**:
   ```bash
   exit
   ```

6. **Verify the Outputs**:
   On your host machine, check the `outputs/` directory for the resulting files.

7. **Stop the Spark Cluster**:
   ```bash
   docker-compose down
   ```
## **Overview**

In this assignment, you will leverage Spark Structured APIs to analyze a dataset containinguser listening behavior and music trends. Your goal is to extract meaningful insights related to user genre preferences, song popularity, and listener engagement. This exercise is designed to enhance your data manipulation and analytical skills using Spark's powerful APIs.

## **Objectives**

By the end of this assignment, you should be able to:

1. **Data Loading and Preparation**: Import and preprocess data using Spark Structured APIs.
2. **Data Analysis**: Perform complex queries and transformations to address specific business questions.
3. **Insight Generation**: Derive actionable insights from the analyzed data.

## **Dataset**

## **Dataset: listening_logs.csv **

You will work with a dataset containing information about user music listening data. The dataset includes the following columns:

| Column Name     | Type    | Description                                           |
|-----------------|---------|-------------------------------------------------------|
| user_id         | String  | ID of the user who listened to the song               |
| song_id         | String  | ID of the song listened to                            |
| listen_time     | Integer | Time spent listening to the song in seconds           |
| genre           | String  | Genre of the song                                     |
| mood            | String  | Mood of the song (Happy, Sad, Neutral, etc.)          |
| timestamp       | String  | Timestamp of the listening event                      |



---

## **Dataset: songs_metadata.csv **
| Column Name | Type    | Description                          |
|-------------|---------|--------------------------------------|
| song_id     | String  | Unique ID for the song               |
| title       | String  | Title of the song                    |
| artist      | String  | Artist of the song                   |
| genre       | String  | Genre of the song                    |
| mood        | String  | Mood of the song (Happy, Sad, etc.)  |

---

### **Sample Data**

Below is a snippet of the `listening_logs.csv`,`songs_metadata.csv` to illustrate the data structure. Ensure your dataset contains at least 100 records for meaningful analysis.

```
user_id,song_id,timestamp,duration_sec
user_82,song_8,2025-03-02 05:08:22,170
user_32,song_15,2025-03-07 18:34:24,82
user_87,song_48,2025-03-27 11:23:53,74
user_76,song_28,2025-03-02 13:01:46,45
user_12,song_14,2025-03-12 07:04:10,288
```

---

```
song_id,title,artist,genre,mood
song_1,Title_song_1,Artist_13,Classical,Chill
song_2,Title_song_2,Artist_2,Rock,Energetic
song_3,Title_song_3,Artist_13,Rock,Chill
song_4,Title_song_4,Artist_2,Classical,Energetic
song_5,Title_song_5,Artist_8,Pop,Chill
```

---



## **Assignment Tasks**

You are required to complete the following tasks using Spark Structured APIs:

### **1. User Favorite Genres (user_favorite_genres)**

**Objective:**  
This task calculates the most frequently listened genre per user, identifying their favorite genre.

**Example Output:**

| user_id   | genere                  | Frequency    |
|-----------|-------------------------|--------------|
| user_1    | Pop                     | 4            |
| user_10   | Classical               | 3            |
| user_10   | Pop                     | 3            |


---

### **2. Average Listen Time Per Song (avg_listen_time_per_song) **

**Objective:**
This task calculates the average listening time for each song based on the listen_time field from the logs.

**Example Output:**

| song_id     |Average|
|-------------|-------|
| song_19     | 201.2 |
| song_47     | 169.9 |
| song_38     | 166.4 |

---

### **3. Top Songs This Week (top_songs_this_week)**

**Objective:**  
This task identifies the top 10 most-played songs during the week of March 24-28, 2025.
**Example Output:**

| song_id        | count       |
|----------------|-------------|
| song_9         | 8           |
| song_30        | 6           |

---

### **4. Happy Song Recommendations (happy_recommendations)**

**Objective:**  
This task recommends happy songs to users who primarily listen to sad songs. It ensures that the recommended songs haven't been previously listened to by the user.

**Example Output:(JSON format)**
```json
{"user_id":"user_1","recommended_songs":[{"song_id":"song_6","title":"Title_song_6","artist":"Artist_9"},{"song_id":"song_8","title":"Title_song_8","artist":"Artist_9"},{"song_id":"song_11","title":"Title_song_11","artist":"Artist_14"}]}
{"user_id":"user_100","recommended_songs":[{"song_id":"song_8","title":"Title_song_8","artist":"Artist_9"},{"song_id":"song_11","title":"Title_song_11","artist":"Artist_14"},{"song_id":"song_16","title":"Title_song_16","artist":"Artist_5"}]}
{"user_id":"user_18","recommended_songs":[{"song_id":"song_6","title":"Title_song_6","artist":"Artist_9"},{"song_id":"song_8","title":"Title_song_8","artist":"Artist_9"},{"song_id":"song_11","title":"Title_song_11","artist":"Artist_14"}]}
```
---

### **5. Genre Loyalty Scores (genre_loyalty_scores)**

**Objective:**  
This task identifies users who show loyalty to a specific genre by calculating a loyalty score based on their listening history.

**Example Output:**

| user_id   | genere                  | loyalty_score|
|-----------|-------------------------|--------------|
| user_39   | Hip-Hop                 | 0.8          |


---


### **6. Night Owl Users (night_owl_users)**

**Objective:**  
This task identifies users who listen to music between 12 AM and 5 AM, and have played more than 5 songs during these hours.

**Example Output:**

| user_id        | time        |
|----------------|-------------|
| user_94        | 7           |
| user_97        | 6           |

---


## 📬 Submission Checklist

- [ ] PySpark scripts are present or not  
- [ ] Output files in the `outputs/` directory  
- [ ] Datasets are generated 
- [ ] Completed `README.md`  
- [ ] Commit everything to GitHub Classroom  
- [ ] Submit your GitHub repo link on canvas

---
