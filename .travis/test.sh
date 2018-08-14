make configure_databricks & # send the long living command to background!

minutes=0
limit=3
while kill -0 $! >/dev/null 2>&1; do
  echo -n -e " \b" # never leave evidences!

  if [ $minutes == $limit ]; then
    break;
  fi

  minutes=$((minutes+1))

  sleep 60
done

exit 0