FROM public.ecr.aws/lambda/python:3.11

WORKDIR /var/task

# Install system dependencies for Chromium, Selenium, and Rust (needed for tiktoken)
RUN yum install -y \
    chromium \
    chromium-driver \
    gcc \
    curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && yum clean all

# Make Rust available
ENV PATH="/root/.cargo/bin:${PATH}"

# Upgrade pip first
RUN pip install --upgrade pip

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN which chromium || which chromium-browser || echo "Chrome not found"
RUN which chromedriver || echo "ChromeDriver not found"
RUN find / -name "chromedriver" 2>/dev/null || echo "chromedriver not found anywhere"

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