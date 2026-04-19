# Bright TV User Viewership Analytics

## Project Overview
This project analyses user viewership behaviour by combining demographic and behavioural data to uncover key insights that drive engagement, content performance, and strategic decision-making.

The goal is to move beyond raw data and provide **actionable insights** that can improve user retention, optimise content scheduling, and enhance overall platform performance.

---

## Data Sources

### 1. User Profile (Demographics)
- UserID
- Age
- Gender
- Race
- Province

### 2. Viewership (Behaviour)
- Channel
- Viewing Timestamp
- Duration
- Time-based attributes (Hour, Day, Month)

---

## Data Transformation

- Joined datasets using `UserID`
- Converted UTC timestamps to South African time
- Created derived fields:
  - Age Groups
  - Time Buckets (Morning, Afternoon, Evening, Night)
  - Duration in Seconds
  - Day of Week & Month

---

## Key Business Questions

- Who is the core audience?
- When do users engage the most?
- Which regions drive the most activity?
- What content performs best?
- Which content retains users longer?
- Why does engagement drop on certain days?

---

## Analysis & Visualisations

The analysis was performed using **SQL and Excel Pivot Tables**, focusing on:

### 1. Core Audience
- Identified dominant age group driving engagement

### 2. Regional Demand
- Analysed user activity by province

### 3. Peak Viewing Time
- Identified highest engagement periods

### 4. Day-of-Week Trends
- Compared weekday vs weekend performance

### 5. Content Popularity
- Top-performing channels by viewership

### 6. Engagement Quality
- Average session duration per channel

### 7. Popularity vs Engagement
- Compared high-traffic vs high-retention content

### 8. Content Preferences by Gender
- Analysed viewing patterns across genders

### 9. Low-Day Strategy
- Identified underperformance on Mondays & Tuesdays

---

## Key Insights

- The **18–35 age group** drives the majority of engagement  
- Viewing activity **peaks in the evening**  
- Engagement drops significantly at the **start of the week**  
- A small number of channels dominate viewership  
- **High traffic ≠ high engagement** — some content retains users better  

---

## Recommendations

- Focus content and marketing on the core audience (18–35)
- Schedule premium content during evening peak hours
- Improve early-week engagement with stronger content placement
- Prioritise high-retention content over just high-traffic content
- Use audience segmentation for personalised content strategies

---

## Tools Used

- SQL (Data extraction & transformation)
- Excel (Pivot tables & visualisations)

---

## Project Outcome

This project demonstrates how data can be transformed into actionable insights that support:
- Strategic decision-making
- Content optimisation
- User engagement growth

---

## Future Improvements

- Build an interactive dashboard using Power BI or Google Looker  
- Implement real-time data tracking  
- Develop a recommendation engine based on user behaviour  

---

## Author

**Sibulelo Mafrika**

---# Bright_TV_Analysis
