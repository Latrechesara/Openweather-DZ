# Module 06: Real-Time Visual Analytics Dashboard

This module details building a high-impact, modern dark-mode dashboard using **Plotly Dash** and **Bootstrap Components**. The dashboard connects directly to ClickHouse to provide real-time visual analytics for Algerian cities.

---

## 1. Dashboard Architecture & Features

```
+---------------------+
| ClickHouse Database |
+----------+----------+
           |
           | (SQL Query / 5-second Poll)
           v
+----------+----------+
| Dash Server App     | <---> Dark Mode Theme (CYBORG + Custom Glassmorphism CSS)
| (src/app.py)        |
+----------+----------+
           |
           v
+---------------------+
| Interactive Web UI  | ---> Live KPI Cards, Heatmap Charts, Stream Line Series
+---------------------+
```

### Key UI Features
* **Modern Aesthetic:** Deep charcoal/black themes with neon accents (cyan, amber, coral) and glassmorphism cards.
* **Auto-Refreshing Callback:** Periodically queries ClickHouse every 5 seconds to render live updates seamlessly without page reloads.
* **Algerian Analytical Focus:** Tracks current temperatures, humidity levels, pressure, wind speeds, and streaming trends across major regional hubs (Algiers, Oran, Constantine, Annaba, Sétif, Ouargla, Ghardaïa, Tamanrasset).

---

## 2. Dependencies Installation

Install the required visualization and database client libraries using `uv`:

```bash
uv add dash dash-bootstrap-components plotly clickhouse-connect pandas
```

---

## 3. Dashboard Implementation (`src/app.py`)

Create or update `src/app.py` with the following code src.app.py



---

## 4. Launching the Live Application

Rerun the docker compose d -up 

again to integrate the dashoard with rest of containers


3. **Access the Web Dashboard**:
   Navigate to **`http://localhost:8050`** in your browser to view the live dark-mode dashboard.