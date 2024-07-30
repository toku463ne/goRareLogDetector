#!/bin/bash

set -e

# Define the project directory under the home directory
PROJECT="goRareLogDetector"
GO_VERSION="1.22.5"

# Function to install Go
install_go() {
    echo "Installing Go..."
    wget https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz -O go.tar.gz
    sudo tar -C /usr/local -xzf go.tar.gz
    rm go.tar.gz

    # Add Go to PATH
    grep "/usr/local/go/bin" ~/.profile &>/dev/null || echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.profile
    source ~/.profile
}

# Function to initialize a Go module
initialize_go_module() {
    echo "Initializing Go module..."
    rm -f go.mod
    rm -f go.sum
    go mod init $PROJECT
    go mod tidy
}

# Function to install necessary Go libraries
install_go_libraries() {
    echo "Installing necessary Go libraries..."
    go get "github.com/sirupsen/logrus"
    go get "github.com/pkg/errors"
    go get "github.com/go-ini/ini"
    go get "goRareLogDetector/pkg/csvdb"
}

# Check if Go is already installed
if ! command -v go &> /dev/null
then
    install_go
else
    echo "Go is already installed."
fi

# Initialize Go module
initialize_go_module

# Install necessary Go libraries
install_go_libraries

echo "Setup complete."
