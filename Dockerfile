# Stage 1: Build dependencies and create wheels (optional, for faster installs)
FROM python:3.11-bullseye AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt \
    && rm -rf /root/.cache/pip

# Stage 2: Production image
FROM python:3.11-bullseye

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=builder /wheels /wheels
COPY requirements.txt .

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels /root/.cache/pip

# Create a custom user with UID 1234 and GID 1234
RUN groupadd -g 1234 pygroup \
    && useradd -m -u 1234 -g pygroup pyuser \
    && chown -R pyuser:pygroup /home/pyuser
   # && mkdir -p /logs/python \
   # && chmod -R 777 /logs

# Switch to the custom user
USER pyuser

# Set the workdir
WORKDIR /home/pyuser
RUN mkdir -p /home/pyuser/.dbt
COPY profiles.yml /home/pyuser/.dbt

ENV PATH="/home/pyuser/.local/bin:$PATH"

CMD ["sleep", "infinity"]
