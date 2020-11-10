from datetime import datetime, timedelta
import trading_calendars

ANNUAL_DAYS = 240

# Get public holidays data from Shanghai Stock Exchange
cn_calendar = trading_calendars.get_calendar('XSHG')
holidays = [x.to_pydatetime() for x in cn_calendar.precomputed_holidays]

# Filter future public holidays
start = datetime.today()
PUBLIC_HOLIDAYS = [x for x in holidays if x >= start]

""" modify by loe """
# 修正到期天数计算错误
def calculate_days_to_expiry(option_expiry: datetime) -> int:
    """"""
    current_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = 0

    while current_dt <= option_expiry:
        # Ignore weekends
        is_trading_day = True
        if current_dt.weekday() in [5, 6]:
            is_trading_day = False

        # Ignore public holidays
        if current_dt in PUBLIC_HOLIDAYS:
            is_trading_day = False

        if is_trading_day:
            days += 1
        current_dt += timedelta(days=1)

    return days
