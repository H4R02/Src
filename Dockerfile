# Base image
FROM golang:1.21-alpine

# Set working directory
WORKDIR /app

# Install FFmpeg and Git (Required for video thumbnails and fetching Go modules)
RUN apk add --no-cache ffmpeg git

# Copy go mod and sum files (If you don't have go.mod, render will build it directly, but copying source is needed)
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
