ps ax | grep "[p]ython ./daemon"
n=`ps ax | grep "[p]ython ./daemon" -c`
echo "found $n daemons running"
