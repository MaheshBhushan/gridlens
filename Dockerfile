# Local Spark + Delta runtime: Python 3.12 with a Java 21 runtime copied from Temurin.
FROM eclipse-temurin:21-jre AS jre
FROM python:3.12-slim
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:/root/.local/bin:$PATH \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    GRIDLENS_IVY_DIR=/opt/ivy \
    TZ=UTC
RUN pip install --no-cache-dir uv==0.11.24
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --extra local --group dev
# Resolve the Delta jars once at build time so runs work offline.
RUN uv run python -c "from gridlens.spark import get_spark; get_spark().stop()"
COPY tests ./tests
ENTRYPOINT ["uv", "run", "--frozen", "--extra", "local", "--group", "dev"]
