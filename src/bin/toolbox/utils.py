import datetime
from dateutil.relativedelta import relativedelta
import calendar

def last_day_of_month(date):
    """
    Find the last day of the month of `date`. For example, if `date` is 2020-11-15, then
    the date 2020-11-30 is returned
    
    :param date: the datetime.date to look at
    :returns: a datetime.date of the last day of the month
    """
    
    first_day, last_day = calendar.monthrange(date.year, date.month)
    return datetime.date(date.year, date.month, last_day)


def first_day_of_month(date):
    """
    Find the first day of the month of `date`. For example, if `date` is 2020-12-15, then
    the date 2020-12-01 is returned
    
    :param date: the datetime.date to look at
    :returns: a datetime.date of the first of the month
    """
    return date.replace(day=1)


def last_day_of_n_months_ago(date, num_months):
    """
    Returns the first day of the month `num_months` ago from `date`
    
    :param date: the initial date (datetime.date)
    :param num_months: the number of months to go back
    :returns: the first day of the month `num_months` ago from `date`
    """

    # Don't collide with months where `date.day` does not exist in month 
    # `date.month - 1` (or any previous month)
    current_date = first_day_of_month(date)

    if num_months == 0:
        current_date = last_day_of_month(current_date)
    else:
        for _ in range(num_months):
            if current_date.month == 1:
                date_aux = datetime.date(current_date.year-1, 12, 1)
                current_date = last_day_of_month(date_aux)
            else:
                date_aux = datetime.date(current_date.year, current_date.month-1, 1)
                current_date = current_date = last_day_of_month(date_aux)

    return current_date


def first_day_of_n_months_ago(date, num_months):
    """
    Returns the first day of the month `num_months` ago from `date`
    
    :param date: the initial date (datetime.date)
    :param num_months: the number of months to go back
    :returns: the first day of the month `num_months` ago from `date`
    """

    # Don't collide with months where `date.day` does not exist in month 
    # `date.month - 1` (or any previous month)
    current_date = first_day_of_month(date)

    for _ in range(num_months):
        if current_date.month == 1:
            current_date = datetime.date(current_date.year-1, 12, 1)
        else:
            current_date = datetime.date(current_date.year, current_date.month-1, 1)

    return current_date


def days_between(start_date, end_date, format_str="%Y-%m-%d"):
    """
    A list of days between a start and end date inclusive
    
    :param start_date: the beginning datetime.date
    :param end_date: the ending datetime.date
    :param format_str: the format the date strings
    :returns: a list of dates as strings
    """
    
    if not (start_date < end_date):
        raise ValueError("Start date must come before end date")

    delta = end_date - start_date
    days = []
    for i in range(delta.days + 1):
        new_day = start_date + datetime.timedelta(days=i)
        days.append(new_day) 

    # Map datetimes to strings
    date_strings = list(map(lambda d: d.strftime(format_str), days))

    return date_strings
