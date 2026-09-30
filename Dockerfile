# Base image updated to fetch the latest Go version automatically
FROM golang:alpine

# Set working directory
WORKDIR /app

# Install FFmpeg and Git (Required for video thumbnails and fetching Go modules)
RUN apk add --no-cache ffmpeg git

# Copy source code
COPY . .

# Initialize go mod if it doesn't exist, and tidy dependencies
RUN go mod init tg_cloner || true
RUN go mod tidy

# Build the Go app
RUN go build -o bot src.go

# Expose the default Render Port
EXPOSE 8080

# Command to run the executable
CMD ["./bot"]
