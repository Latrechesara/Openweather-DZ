---
video_url: "https://www.youtube.com/watch?v=YOUR_VIDEO_ID"
prev_url: 01-intro.md
next_url: 03-data.md
---

## 🛠️ Built With

<p align="center">
  <!-- Languages & Environments -->
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/SQL-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQL" />
  <img src="https://img.shields.io/badge/Shell-4E2A8E?style=for-the-badge&logo=gnu-bash&logoColor=white" alt="Shell" />
  <br>

  <!-- Containerization & Streaming -->
  <img src="https://img.shields.io/badge/Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker Compose" />
  <img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Apache Kafka" />
  <img src="https://img.shields.io/badge/ClickHouse-FFCC00?style=for-the-badge&logo=clickhouse&logoColor=black" alt="ClickHouse" />
  <br>

  <!-- Package Management & Visualization -->
  <img src="https://img.shields.io/badge/uv-Astral-261230?style=for-the-badge&logo=python&logoColor=white" alt="uv" />
  <img src="https://img.shields.io/badge/Plotly_Dash-3F4F75?style=for-the-badge&logo=plotly&logoColor=white" alt="Plotly Dash" />
  <img src="https://img.shields.io/badge/VS_Code-007ACC?style=for-the-badge&logo=visualstudiocode&logoColor=white" alt="VS Code" />
</p>

# Environment Setup

In this module, we'll set up our development environment for real-time event streaming using modern tooling from scratch, step by step.

### 1. Prerequisites & Editor Setup

First, make sure you have Visual Studio Code installed. If not, download it from the official site:

[Download VS Code](https://code.visualstudio.com/Download)

Choose the correct installer for your operating system (Windows, macOS, or Linux).

### 2. Install Docker & Docker Desktop

Since our architecture runs multi-container services (`Kafka KRaft`, `ClickHouse`, `Producer`, `Consumer`, `Dash`), you must have **Docker Desktop** installed and running.

- **Windows / macOS:** Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
- **Linux:** Install `docker` and `docker-buildx` / `docker-compose-plugin` via your distribution's package manager.

Verify your installation in terminal:

```bash
docker --version
docker compose version
```

### 3. Installing uv (Python Package Manager)

We will use **uv** by Astral as our ultra-fast Python package and workspace manager.

#### On Windows (PowerShell):
```powershell
irm https://astral.sh/uv/install.ps1 | iex
```

#### On macOS / Linux:
```bash
curl -sSf https://astral.sh/uv/install.sh | sh
```

After installation, restart your terminal and verify it works:

```bash
uv --version
```

### 4. Project Initialization

Next, we initialize a clean Python workspace using `uv`:

```bash
uv init --bare
```

This generates your initial configuration files, including `pyproject.toml`.

Next, add all required packages for our streaming data pipeline using `uv add`:

```bash
uv add kafka-python requests dash dash-bootstrap-components pandas plotly clickhouse-connect
```

### 5. OpenWeather API Key Setup

Our data producer requires access to live weather telemetry. 

1. Create a free account on [OpenWeatherMap](https://home.openweathermap.org/users/sign_up).
2. Generate a free API Key under your account settings.
3. Store your key in an environment variable in your terminal:

#### Windows (PowerShell):
```powershell
$env:OPENWEATHER_API_KEY="your_actual_api_key_here"
```

#### macOS / Linux:
```bash
export OPENWEATHER_API_KEY="your_actual_api_key_here"
```

---

## 🎯 Module Breakdown

This environment module consists of two parts:

- **Part 1:** Installing local tools (`VS Code`, `Docker Desktop`, `uv`) and creating the Python workspace dependencies.
- **Part 2:** Configuring `docker-compose.yml`, booting the `KRaft Kafka` broker alongside `ClickHouse`, and verifying service health before deploying python producers/consumers.

The full configuration and setup code for this module is available in the `code/` directory.