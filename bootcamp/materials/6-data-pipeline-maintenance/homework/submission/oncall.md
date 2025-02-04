# Data Pipeline Runbooks

## Overview
This document provides runbooks for managing key data pipelines for investor reporting, including ownership, on-call schedules, and common issues.

## Pipeline: Profit
### Owners:
- **Primary Owner:** Finance Team/Risk Team
- **Secondary Owner:** Data Engineering Team

### Common Issues:
- Numbers not aligning with account filings; requires verification by an accountant.

### SLA:
- Monthly review by the accounts team.

### On-call Schedule:
- Monitored by BI in the profit team.
- Weekly rotation for pipeline monitoring.
- Fixes required if the pipeline breaks.

---

## Pipeline: Growth
### Owners:
- **Primary Owner:** Accounts Team
- **Secondary Owner:** Data Engineering Team

### Common Issues:
- Missing time-series data due to an incomplete account status update.
- Identifiable by missing intermediate steps in account change history.

### SLA:
- Latest account statuses to be available by the end of the week.

### On-call Schedule:
- No immediate on-call support.
- Debugging occurs during working hours.

---

## Pipeline: Engagement
### Owners:
- **Primary Owner:** Software Frontend Team
- **Secondary Owner:** Data Engineering Team

### Common Issues:
- Late-arriving click data in Kafka causing missing aggregations.
- Kafka outages leading to missing user event data.
- Duplicate events in the pipeline requiring de-duplication.

### SLA:
- Data must arrive within 48 hours.
- Issues must be resolved within a week.

### On-call Schedule:
- One data engineer assigned weekly.
- Weekly 30-minute knowledge transfer meeting for the next owner.

---

## Pipeline: Aggregated Data for Executives and Investors
### Owners:
- **Primary Owner:** Business Analytics Team
- **Secondary Owner:** Data Engineering Team

### Common Issues:
- Spark joins failing due to large data volume (OOM errors).
- Stale data in upstream pipelines affecting accuracy.
- Missing data leading to NA values or divide-by-zero errors.

### SLA:
- Issues resolved by end-of-month reporting deadline.

### On-call Schedule:
- Increased monitoring in the last week of the month to ensure smooth reporting.

---

## Fair On-call Schedule Considerations:
- Rotations should account for holiday coverage to ensure fair workload distribution.
- Senior engineers should support junior engineers during escalations.
- Documentation should be maintained to streamline onboarding for new engineers in the rotation.

