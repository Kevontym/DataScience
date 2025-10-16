#!/bin/bash

# Data Cleaner Docker Helper Script

echo "🚀 Data Cleaner Docker Manager"

case "$1" in
    "build")
        echo "📦 Building Docker image..."
        docker build -t data-cleaner .
        ;;
    "run")
        echo "🎯 Running Data Cleaner interactively..."
        docker run -it \
            -v $(pwd)/data:/app/data \
            -v $(pwd)/reports:/app/reports \
            -v $(pwd)/outputs:/app/outputs \
            data-cleaner
        ;;
    "sql")
        echo "🗃️ Opening SQLite terminal..."
        docker run -it \
            -v $(pwd)/report_gen.db:/app/report_gen.db \
            data-cleaner sqlite3 report_gen.db
        ;;
    "compose")
        echo "🐳 Starting with Docker Compose..."
        docker-compose up
        ;;
    "compose-sqlite-web")
        echo "🌐 Starting SQLite web interface on http://localhost:8080"
        docker-compose up sqlite-web
        ;;
    "clean")
        echo "🧹 Cleaning up Docker resources..."
        docker system prune -f
        ;;
    *)
        echo "Usage: $0 {build|run|sql|compose|compose-sqlite-web|clean}"
        echo ""
        echo "Commands:"
        echo "  build                 - Build Docker image"
        echo "  run                   - Run interactively with volume mounts"
        echo "  sql                   - Open SQLite terminal for report_gen.db"
        echo "  compose               - Start with Docker Compose"
        echo "  compose-sqlite-web    - Start SQLite web interface"
        echo "  clean                 - Clean up Docker resources"
        ;;
esac