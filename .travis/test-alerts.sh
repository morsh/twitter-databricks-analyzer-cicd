set -e

make test_alerts & # send the long living command to background!
PROC_ID=$!

# Constants
RED='\033[0;31m'
minutes=0
limit=30

while kill -0 "$PROC_ID" >/dev/null 2>&1; do
  echo -n -e " \b" # never leave evidences!

  if [ $minutes == $limit ]; then
    echo -e "\n"
    echo -e "${RED}Test has reached a ${minutes} minutes timeout limit"
    exit 1
  fi

  minutes=$((minutes+1))

  sleep 60
done

echo "exit code for script: $?"
exit $?