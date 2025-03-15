# Use Node.js LTS as the base image
FROM node:20-alpine AS base

# Set working directory
WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Install dependencies
FROM base AS deps
COPY pnpm-lock.yaml* package.json ./
RUN pnpm install --frozen-lockfile

# Build the application
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN pnpm build

# Production image
FROM base AS runner
ENV NODE_ENV production

# Ensure pnpm is available in production
RUN npm install -g pnpm

# Create a non-root user
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
USER nextjs

# Copy necessary files
COPY --from=builder --chown=nextjs:nodejs /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

# Expose port
EXPOSE 3000

# Set environment variables
ENV PORT 3000
ENV HOSTNAME "0.0.0.0"

# Start the application
CMD ["node", "server.js"]