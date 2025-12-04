# Use Node.js LTS
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy the rest of the app
COPY . .

# Copy .env (optional: you can also use Quix secrets)
# COPY .env .env

# Expose port (optional, not needed for Kafka producer)
# EXPOSE 3000

# Run the app
CMD ["node", "index.js"]
