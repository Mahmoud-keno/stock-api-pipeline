# this script is running on linux
from crontab import CronTab
from script import fetch_data
def cron_job():
    cron = CronTab(user=True)  # This is the correct syntax
    job = cron.new(command='python script.py', comment='Fetch stock dividends data every 5 minutes')
    job.minute.every(5)
    cron.write()


if __name__ == "__main__":
    cron_job()


