# Base image specification. Use Debian for stability and consistency
FROM --platform=linux/x86_64 debian:bookworm

# Set environment variables to avoid interactive prompts
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# System dependencies installation. Install essential tools and libraries required for development and runtime
RUN apt-get update && apt-get install -y \
    wget \
    git \
    build-essential \
    clang \
    curl \
    locales \
    tzdata \
    # Required for documentation generation (ronn is mentioned in README)
    ruby \
    ruby-dev \
    # Make is required for the build system
    make \
    && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV LANG en_US.utf8
ENV LC_ALL en_US.UTF-8

# Set default workdir to testbed
WORKDIR /testbed/

# Target Project setup. Clone source code, checkout to the target version, and prepare build environment
RUN git clone https://github.com/beanstalkd/beanstalkd /testbed && \
    chmod -R 777 /testbed

# Install ronn (documentation tool mentioned in README) via gem
RUN gem install ronn

# Note: The specific commit SHA will be provided during build via --build-arg
ARG TARGET_COMMIT
RUN if [ -n "$TARGET_COMMIT" ]; then \
    cd /testbed && \
    git reset --hard "$TARGET_COMMIT" && \
    git remote remove origin; \
    fi

# Make sure the version generation scripts are executable
RUN chmod +x /testbed/vers.sh /testbed/verc.sh

# Print environment info for debugging
RUN gcc --version && \
    make --version && \
    which ronn