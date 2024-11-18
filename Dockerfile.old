# Use the official Golang image to create a build artifact.
# This is the first stage of a multi-stage build.
FROM golang:1.22.5 as builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go app
WORKDIR /app/cmd/scheduler
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /main .

# Start a new stage from scratch
FROM alpine:latest 

# Install necessary packages
RUN apk --no-cache add ca-certificates sudo

# Set the Current Working Directory inside the container
WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /main .

# Command to run the executable
CMD ["./main"]
