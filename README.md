# Real Estate Intelligence Platform

DEPLOYED LINK: https://real-estate-intelligence-platform.streamlit.app/

This project is a dashboard that combines data from different places—like spreadsheets, news sites, and housing prices—and turns them into easy-to-read charts and insights. 

Instead of looking at raw rows of data, this tool helps you instantly see what is happening in the real estate market, like where money is going and what news reporters are talking about. You can even ask the system questions in plain English to get facts about the data.

## What Does It Do?

1. **Combines Data:** It takes different formats like Excel files, CSVs, and web articles and puts them together into a single JSON file.
2. **Beautiful Dashboard:** It displays the data through a clean and modern web app with 3 main charts.
3. **Chat Assistant:** It has an AI assistant that you can ask questions about the property deals and news.

## How to Install and Run

### What You Need First
- Python 3.10 or newer.
- An API Key from Financial Modeling Prep (FMP) for public stock data.
- An AI API Key (for example, Google Gemini or OpenAI) to power the chat and insights.

### 1. Download the code
```bash
git clone https://github.com/yourusername/real_estate_pipeline.git
cd real_estate_pipeline
```

### 2. Set up the Python environment
```bash
python -m venv venv

# If you are on Mac/Linux, run:
source venv/bin/activate  

# If you are on Windows, run:
venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Add your Data Files
Make sure you create a folder named `data` inside the main project folder. Put your CSVs (`homes.csv`, `zillow.csv`, `cities.csv`,`Real-Estate-Capital-Europe-Sample-CRE-Lending-Data.xlsx`) and your commercial real estate Excel file into this `data` folder.

### 4. Create your `secrets.toml` File
Create a new file named `secrets.toml` in the same folder as `.streamlit`. Open it and add these lines with your actual keys:
```
FMP_API_KEY="your_fmp_api_key"
GEMINI_API_KEY="your_api_key_here"
GROQ_MODEL="your_model_name"
```

### 5. Run the Data Pipeline
Before you can see the dashboard, you need to process all the spreadsheets and articles. Run this command:
```bash
python pipeline.py
```
This takes about 2 minutes. It reads your `data` folder, scrapes news websites, asks the AI to find key details, and saves everything into a `unified_dataset.json` file.

### 6. Start the Dashboard
```bash
streamlit run app.py
```
Your browser will open to `localhost:8501`, and you can use the app!

---

## What the Charts Show and Why They Were Added

### 1. Deals by Asset Class & Region (Pie & Bar Charts)
**What it is:** These charts show exactly where the commercial real estate loans are going. They group the money by property type (like Office, Retail, or Residential) and by location (UK vs Europe).

**What it tells you:** It lets you see at a glance what kinds of properties are getting the most funding. 

**Why it was added:** We need to know the basic facts before digging deeper. This is the simplest way to show exactly what is in the main dataset so the user knows what they are looking at.

<img width="1785" height="889" alt="image" src="https://github.com/user-attachments/assets/b6807231-edc0-44d5-aa01-e94ebc557dd6" />

### 2. Asset Class Gap: Media Attention vs Capital Flow (Bar Chart)
**What it is:** This chart puts two totally different data sources next to each other. The blue bars show how often a property type is mentioned in the news (JLL, Altus, Property Week). The green bars show how many actual loan deals happened for that property type.

**What it tells you:** It shows the "Coverage Gap." For example, maybe the news talks about Office space constantly, but the actual money is going into Industrial warehouses. 

**Why it was added:** I wanted to show that the system can do more than just read an Excel file. By comparing news hype to real money, it pulls out a smart, unique insight that you couldn't get without combining these two data sources.

<img width="1777" height="591" alt="image" src="https://github.com/user-attachments/assets/19aca472-79da-4347-bdf8-928dcc7b3849" />

### 3. Institutional Capital & Deal Fluidity (Histogram / Distribution)
**What it is:** This is a chart showing the size of all the different commercial loans in USD. Next to it, there is a card showing the Average deal size, the Median deal size, and the Largest deal.

**What it tells you:** By comparing the Average and the Median, you can see how "skewed" the market is. If the Average is way higher than the Median, it means a few massive mega-deals are pulling the numbers up, while most normal deals are much smaller.

**Why it was added:** When doing data analysis, looking at just the "Average" can be misleading. Showing the distribution gives a much clearer and more honest picture of how much money is normally being spent on a daily basis.

<img width="1778" height="535" alt="image" src="https://github.com/user-attachments/assets/b3869854-462b-4329-9328-67037f42ec92" />

---

## How It Works Behind the Scenes

- **Speed over Live Updates:** Instead of scraping websites and running AI while the user waits for the webpage to load, I created `pipeline.py`. You run it once to bake everything into a JSON file. This makes the Streamlit dashboard lightning-fast.
- **Why no SQL Database?** For this specific project, saving everything into a structured JSON file is perfectly fine. It allows the Python AI helper (`ai_utils.py`) to quickly search through the entries to answer chat questions without the complexity of a huge database setup.
- **Custom Design:** Streamlit apps usually look a little plain out of the box. I added custom CSS to change fonts and colors so the final product looks like a premium, professional web application.
