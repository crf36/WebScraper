FROM public.ecr.aws/lambda/python:3.11

WORKDIR /var/task

# Install system dependencies for Chromium, Selenium, and Rust (needed for tiktoken)
RUN yum install -y \
    gcc \
    curl \
    unzip \
    && curl -L -o /tmp/google-chrome.rpm https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm \
    && yum install -y /tmp/google-chrome.rpm \
    && rm -f /tmp/google-chrome.rpm \
    && CHROME_MAJOR=$(google-chrome --version | awk '{print $3}' | cut -d. -f1) \
    && DRIVER_VERSION=$(curl -fsSL https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_MAJOR}) \
    && curl -fsSL -o /tmp/chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/${DRIVER_VERSION}/linux64/chromedriver-linux64.zip \
    && unzip -q /tmp/chromedriver.zip -d /tmp \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/chromedriver.zip /tmp/chromedriver-linux64 \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && yum clean all

# Make Rust available
ENV PATH="/root/.cargo/bin:${PATH}"

# Upgrade pip first
RUN pip install --upgrade pip

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN which google-chrome || which chromium || which chromium-browser || echo "Chrome not found"
RUN which chromedriver || echo "ChromeDriver not found"
RUN google-chrome --version || echo "Chrome version unavailable"
RUN chromedriver --version || echo "ChromeDriver version unavailable"

# Copy all application files
COPY main_scraper.py .
COPY selenium_scraper.py .
COPY ta_scraper.py .
COPY ai_processor.py .
COPY google_reviews_api.py .
COPY lambda_handler.py .

CMD ["lambda_handler.handler"]


# COMMAND TO BUILD DOCKER CONTAINER
# docker build -t scraper-lambda .

# COMMAND TO RUN CONTAINER
# docker run --rm --env-file .env -p 9000:8080 scraper-lambda

# COMMAND TO HIT CONTAINER (DO THIS FROM A DIFFERENT TERMINAL WHILE THE CONTAINER IS RUNNING)
# curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" -H "Content-Type: application/json" -d "{\"city\": \"Austin\"}"