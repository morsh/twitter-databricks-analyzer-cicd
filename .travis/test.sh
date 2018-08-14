make configure_databricks & # send the long living command to background!

# Constants
RED='\033[0;31m'
minutes=0
limit=3

while kill -0 $! >/dev/null 2>&1; do
  echo -n -e " \b" # never leave evidences!

  if [ $minutes == $limit ]; then
    echo -e "\n"
    echo -e "${RED}Test has reached the ${minutes} timeout limit"
    exit 1
  fi

  minutes=$((minutes+1))

  sleep 60
done

exit 0