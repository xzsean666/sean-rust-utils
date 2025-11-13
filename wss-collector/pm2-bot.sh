#!/bin/bash

# Function to check and install pm2
check_and_install_pm2() {
    if ! command -v pm2 &> /dev/null; then
        echo "pm2 not installed. Attempting global installation using npm..."
        if command -v npm &> /dev/null; then
            npm install pm2 -g
            if [ $? -ne 0 ]; then
                echo "npm install pm2 failed. Trying yarn..."
                if command -v yarn &> /dev/null; then
                    yarn global add pm2
                    if [ $? -ne 0 ]; then
                        echo "yarn install pm2 failed. Trying pnpm..."
                        if command -v pnpm &> /dev/null; then
                            pnpm install pm2 -g
                            if [ $? -ne 0 ]; then
                                echo "pnpm install pm2 failed. Please install pm2 manually (npm install -g pm2, yarn global add pm2, or pnpm install -g pm2)."
                                exit 1
                            fi
                        else
                            echo "pnpm command not found. Please install pm2 manually."
                            exit 1
                        fi
                    fi
                else
                    echo "yarn command not found. Please install pm2 manually."
                    exit 1
                fi
            fi
        else
            echo "npm command not found. Please install pm2 manually."
            exit 1
        fi
        # Check if installation was successful
        if ! command -v pm2 &> /dev/null; then
            echo "pm2 installation failed, please install manually."
            exit 1
        fi
        echo "pm2 installed successfully!"
    fi
}

# Function to setup PM2 startup (run only once)
setup_pm2_startup() {
    # Check if pm2 startup is already configured
    if ! pm2 startup 2>&1 | grep -q "already configured"; then
        echo "Configuring pm2 startup script..."
        echo "Note: You may need to run the following command with sudo if prompted:"
        pm2 startup
        
        if [ $? -ne 0 ]; then
            echo "Warning: pm2 startup configuration may require manual intervention."
            echo "Please check the output above and run the suggested command if needed."
        fi
    fi
}

# Function to build PM2 start command
build_pm2_command() {
    local app_name="$1"
    local start_command="$2"
    local cmd=""
    
    # Check if command is a .js/.ts file (Node.js app)
    if [[ "$start_command" =~ \.(js|ts|mjs|cjs)$ ]]; then
        # Node.js application
        cmd="pm2 start \"$start_command\" --name \"$app_name\" --node-args=\"--max-old-space-size=4096\""
    else
        # For binaries and scripts with arguments, extract the binary and args
        local binary=$(echo "$start_command" | awk '{print $1}')
        local args=$(echo "$start_command" | cut -d' ' -f2-)
        
        # Use -- to pass arguments to the binary itself
        if [ -n "$args" ] && [ "$args" != "$binary" ]; then
            cmd="pm2 start \"$binary\" --name \"$app_name\" -- $args"
        else
            cmd="pm2 start \"$binary\" --name \"$app_name\""
        fi
    fi
    
    echo "$cmd"
}

# Check if parameters are provided
if [ $# -eq 0 ]; then
    echo "Please provide parameters: --start <command> or --stop or --restart [command] or --status or --monit or --logs"
    echo ""
    echo "Usage:"
    echo "  $0 --start <command>       Start bot (e.g.: $0 --start 'node dist/main.js')"
    echo "  $0 --stop                  Stop bot and remove from startup"
    echo "  $0 --restart               Restart running bot"
    echo "  $0 --restart <command>     Restart bot or start new bot (if not running)"
    echo "  $0 --status                Check bot status (including CPU & memory)"
    echo "  $0 --monit                 Real-time monitoring of bot (CPU, memory, etc)"
    echo "  $0 --logs [lines]          View bot logs (default: last 50 lines)"
    echo ""
    echo "Examples:"
    echo "  $0 --start 'node dist/main.js'         # Node.js application"
    echo "  $0 --start dist/main.js                # Auto-detect Node.js file"
    echo "  $0 --start 'python3 bot.py'            # Python application"
    echo "  $0 --start './my-script.sh'            # Bash script"
    echo "  $0 --start 'npm run start'             # NPM script"
    echo "  $0 --status"
    echo "  $0 --monit"
    echo "  $0 --logs"
    echo "  $0 --logs 100"
    exit 1
fi

# Get current directory name
CURRENT_DIR=$(pwd)
DIR_NAME=$(basename "$CURRENT_DIR")
APP_NAME_FILE="bot.name"

case "$1" in
    --start)
        # Check if start command is provided
        if [ -z "$2" ]; then
            echo "Error: Please provide start command"
            echo "Usage: $0 --start <command>"
            echo "Example: $0 --start 'node dist/main.js'"
            exit 1
        fi
        
        START_COMMAND="$2"
        
        # Check and install pm2
        check_and_install_pm2

        # Check if bot is already running
        if [ -f "$APP_NAME_FILE" ]; then
            EXISTING_NAME=$(cat "$APP_NAME_FILE")
            if pm2 list | grep -q "$EXISTING_NAME"; then
                echo "Bot $EXISTING_NAME is already running."
                echo "Use '$0 --stop' to stop, or '$0 --restart' to restart"
                exit 1
            fi
        fi

        # Generate dynamic name and save it
        TIMESTAMP=$(date '+%Y%m%d%H%M%S')
        APP_NAME="${DIR_NAME}-bot-${TIMESTAMP}"
        echo "$APP_NAME" > "$APP_NAME_FILE"
        echo "Bot name saved to $APP_NAME_FILE: $APP_NAME"

        # Setup PM2 startup if not already configured
        setup_pm2_startup

        # Build and execute PM2 command
        PM2_CMD=$(build_pm2_command "$APP_NAME" "$START_COMMAND")
        echo "Executing command: $PM2_CMD"
        eval "$PM2_CMD"
        
        if [ $? -eq 0 ]; then
            # Save PM2 process list for auto-start on boot
            echo "Saving PM2 process list for auto-start on boot..."
            pm2 save
            
            echo "Bot started with pm2:"
            echo "  Name: $APP_NAME"
            echo "  Command: $START_COMMAND"
            echo "  Auto-start: enabled"
            echo ""
            pm2 list
        else
            echo "Failed to start bot with pm2."
            rm "$APP_NAME_FILE" # Clean up name file if start fails
            exit 1
        fi
        ;;

    --stop)
        # Check if name file exists
        if [ ! -f "$APP_NAME_FILE" ]; then
            echo "Bot name file not found ($APP_NAME_FILE)."
            echo "Please manually find and stop pm2 process:"
            echo "  pm2 list"
            echo "  pm2 stop <name|id>"
            echo "  pm2 delete <name|id>"
            exit 1
        fi

        # Read application name from file
        APP_NAME=$(cat "$APP_NAME_FILE")

        # Check if the process exists in pm2 list
        if ! pm2 list | grep -q "$APP_NAME"; then
            echo "Warning: Process $APP_NAME not found in pm2 list."
            echo "It may have been stopped manually. Cleaning up name file..."
            rm "$APP_NAME_FILE"
            exit 0
        fi

        # Stop application with pm2
        echo "Stopping pm2 bot: $APP_NAME..."
        pm2 stop "$APP_NAME"
        pm2 delete "$APP_NAME" # Delete from pm2 list after stopping

        # Update saved PM2 process list (removes from auto-start)
        echo "Removing from auto-start list..."
        pm2 save --force
        
        # Remove name file
        rm "$APP_NAME_FILE"
        echo "pm2 bot $APP_NAME stopped and removed from pm2 list and auto-start."
        ;;

    --restart)
        # Check if name file exists and process is running
        if [ -f "$APP_NAME_FILE" ]; then
            APP_NAME=$(cat "$APP_NAME_FILE")
            if pm2 list | grep -q "$APP_NAME"; then
                # Bot is running, restart it
                echo "Restarting pm2 bot: $APP_NAME..."
                pm2 restart "$APP_NAME" --update-env

                if [ $? -eq 0 ]; then
                    # Save PM2 process list to ensure auto-start is maintained
                    pm2 save
                    
                    echo "pm2 bot $APP_NAME restarted."
                    echo ""
                    pm2 list
                else
                    echo "Failed to restart pm2 bot."
                    exit 1
                fi
                exit 0
            else
                # Bot name file exists but process not running
                echo "Warning: Bot name file found but process not running. Cleaning up old name file..."
                rm "$APP_NAME_FILE"
            fi
        fi

        # No running bot found, check if start command is provided
        if [ -z "$2" ]; then
            echo "No running bot found and no start command provided."
            echo "Please use one of the following commands:"
            echo "  $0 --restart <command>    # Start new bot"
            echo "  $0 --start <command>      # Start new bot"
            exit 1
        fi

        START_COMMAND="$2"

        # Check and install pm2
        check_and_install_pm2

        echo "No running bot found, starting new bot..."

        # Generate dynamic name and save it
        TIMESTAMP=$(date '+%Y%m%d%H%M%S')
        APP_NAME="${DIR_NAME}-bot-${TIMESTAMP}"
        echo "$APP_NAME" > "$APP_NAME_FILE"
        echo "Bot name saved to $APP_NAME_FILE: $APP_NAME"

        # Setup PM2 startup if not already configured
        setup_pm2_startup

        # Build and execute PM2 command
        PM2_CMD=$(build_pm2_command "$APP_NAME" "$START_COMMAND")
        echo "Executing command: $PM2_CMD"
        eval "$PM2_CMD"
        
        if [ $? -eq 0 ]; then
            # Save PM2 process list for auto-start on boot
            echo "Saving PM2 process list for auto-start on boot..."
            pm2 save
            
            echo "Bot started with pm2:"
            echo "  Name: $APP_NAME"
            echo "  Command: $START_COMMAND"
            echo "  Auto-start: enabled"
            echo ""
            pm2 list
        else
            echo "Failed to start bot with pm2."
            rm "$APP_NAME_FILE" # Clean up name file if start fails
            exit 1
        fi
        ;;

    --status)
        if [ -f "$APP_NAME_FILE" ]; then
            APP_NAME=$(cat "$APP_NAME_FILE")
            echo "Current bot name: $APP_NAME"
            echo ""
            if pm2 list | grep -q "$APP_NAME"; then
                echo "Bot status:"
                pm2 show "$APP_NAME"
                echo ""
                echo "======================================"
                echo "CPU and Memory Usage:"
                echo "======================================"
                pm2 list | grep "$APP_NAME"
            else
                echo "Bot is not running in pm2."
            fi
        else
            echo "Bot name file not found. No bot may have been started via this script."
            echo ""
            echo "All pm2 processes:"
            pm2 list
        fi
        ;;

    --monit)
        # Real-time monitoring of bot process
        if [ -f "$APP_NAME_FILE" ]; then
            APP_NAME=$(cat "$APP_NAME_FILE")
            echo "Starting real-time monitoring for bot: $APP_NAME"
            echo "(Press Ctrl+C to exit)"
            echo ""
            pm2 monit
        else
            echo "Bot name file not found. Starting full pm2 monitoring..."
            echo "(Press Ctrl+C to exit)"
            echo ""
            pm2 monit
        fi
        ;;

    --logs)
        # Check if name file exists
        if [ -f "$APP_NAME_FILE" ]; then
            APP_NAME=$(cat "$APP_NAME_FILE")
            if pm2 list | grep -q "$APP_NAME"; then
                # Get lines parameter (default 50)
                LINES="${2:-50}"
                echo "Showing last $LINES lines of logs for bot $APP_NAME:"
                echo "======================================"
                pm2 logs "$APP_NAME" --lines "$LINES"
            else
                echo "Bot $APP_NAME is not running in pm2."
                echo ""
                echo "All pm2 process logs:"
                pm2 logs --lines "${2:-50}"
            fi
        else
            echo "Bot name file not found. Showing all pm2 process logs:"
            echo "======================================"
            pm2 logs --lines "${2:-50}"
        fi
        ;;

    *)
        echo "Invalid parameter. Please use --start <command> or --stop or --restart or --status or --monit or --logs"
        exit 1
        ;;
esac