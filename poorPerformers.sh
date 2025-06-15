#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$(dirname "$0")"

# Define file paths in the script's directory
LIST_CHANNELS_FILE="${SCRIPT_DIR}/listChannels"
FWDING_HISTORY_FILE="${SCRIPT_DIR}/fwdingHistory"
OUTPUT_FILE="${SCRIPT_DIR}/peer_activity_report.csv"

echo "--- Starting Lightning Channel Report Generation ---"

# 1. Run lncli listchannels and save to a temporary file
echo "Fetching active channel data..."
lncli listchannels > "${LIST_CHANNELS_FILE}" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: Failed to run 'lncli listchannels'. Please check your LND setup."
    exit 1
fi
echo "Active channel data fetched."

# 2. Run lncli fwdinghistory and save to a temporary file
echo "Fetching forwarding history data for the last year (max 50000 events)..."
lncli fwdinghistory --start_time -1y --max_events 50000 > "${FWDING_HISTORY_FILE}" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: Failed to run 'lncli fwdinghistory'. Please check your LND setup."
    exit 1
fi
echo "Forwarding history data fetched."

# 3. Activate the virtual environment
echo "Activating virtual environment..."
if [ -f "${SCRIPT_DIR}/venv/bin/activate" ]; then
    source "${SCRIPT_DIR}/venv/bin/activate"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to activate virtual environment. Ensure 'venv' is correctly set up."
        exit 1
    fi
    echo "Virtual environment activated."
else
    echo "Error: Virtual environment not found at ${SCRIPT_DIR}/venv/. Please create it first (python3 -m venv venv)."
    exit 1
fi

# 4. Run the Python script with the generated files
echo "Running Python script to process data..."
python3 "${SCRIPT_DIR}/process_channel_and_forwarding_data.py" "${LIST_CHANNELS_FILE}" "${FWDING_HISTORY_FILE}" "${OUTPUT_FILE}"
if [ $? -ne 0 ]; then
    echo "Error: Python script failed. See above for Python errors."
    # Deactivate venv before exiting if script fails
    deactivate 2>/dev/null
    exit 1
fi
echo "Python script completed successfully."

# 5. Deactivate the virtual environment
echo "Deactivating virtual environment..."
deactivate 2>/dev/null
echo "Virtual environment deactivated."

# 6. Display the bottom 5 open channels (at least 30 days old and with local balance > 0) for consideration
echo ""
echo "--- Channels Recommended for Review (Lowest 'Fees/Days Sats', older than 30 days w/ Local Balance > 0) ---"
# Extract header and filter open channels with Age(Days) >= 30, LocalBalance > 0, sort by Fees/Days Sats ascending, take top 5, and format as a table
# Note:
# - $8 == "True": checks the 'Open' status (8th column)
# - $5 >= 30: checks 'Age(Days)' (5th column)
# - $2 > 0: checks 'LocalBalance' (2nd column) is greater than 0
# - sort -t, -k7 -n: sorts by 'Fees/Days Sats' (7th column) numerically
{
    head -n 1 "${OUTPUT_FILE}"
    awk -F, '$8 == "True" && $5 >= 30 && $2 > 0 {print}' "${OUTPUT_FILE}" | sort -t, -k7 -n | head -n 5
} | column -t -s ','

# Count the number of qualifying channels
# Use awk to count lines where Open is "True" ($8), Age(Days) is >= 30 ($5), and LocalBalance is > 0 ($2)
QUALIFYING_COUNT=$(awk -F, '$8 == "True" && $5 >= 30 && $2 > 0 {count++} END {print count+0}' "${OUTPUT_FILE}")
if [ "$QUALIFYING_COUNT" -lt 5 ]; then
    echo "Note: Only $QUALIFYING_COUNT open channel(s) are at least 30 days old, have a local balance > 0, and qualify for this recommendation."
fi

# 7. Clean up temporary files
echo ""
echo "Cleaning up temporary files..."
rm -f "${LIST_CHANNELS_FILE}" "${FWDING_HISTORY_FILE}"
if [ $? -eq 0 ]; then
    echo "Temporary files deleted successfully."
else
    echo "Warning: Could not delete temporary files. You might need to delete them manually."
fi

echo "--- Report Generation Complete ---"
echo "Your full report is available at: ${OUTPUT_FILE}"

