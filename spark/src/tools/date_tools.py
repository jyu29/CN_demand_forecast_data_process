from datetime import datetime, timedelta


def get_week_id(date_str):
    """
      return the week number of a date (ex: 202051)
       - year: str(date.isocalendar()[0])
       - week number: date.strftime("%V") (return the week number where week is starting from monday)
      If the day is sunday, I add one day to get the good day value in order to respect Decathlon regle:
        the fist day of week is sunday
    """
    date = datetime.strptime(date_str, "yyyy-MM-dd")
    day_of_week = date.strftime("%w")
    date = date if (day_of_week != '0') else date + timedelta(days=1)
    return int(str(date.isocalendar()[0]) + str(date.isocalendar()[1]).zfill(2))


def get_first_day_month(week_id):
    """
    Get the month of the first day of week
    ex: 202122 (30/05 -> 05/06) => result = 2021-05-01
    """
    fdm = (datetime.strptime(str(week_id) + '1', '%G%V%u') - timedelta(days=1)).replace(day=1)
    return fdm


def get_last_day_week(week_id):
    """
    Get the date of the last day of week (saturday)
    """
    ldw = datetime.strptime(str(week_id) + '6', '%G%V%u')
    return ldw


def get_current_week():
    """
    Return current week (international standard ISO 8601 - first day of week
    is Sunday, with format 'YYYYWW'
    :return current week (international standard ISO 8601) with format 'YYYYWW'
    """
    shifted_date = datetime.today()
    current_week_id = get_week_id(shifted_date)
    return current_week_id


def get_shift_n_week(week_id, nb_weeks):
    """
    Return shifted week (previous or next)
    """
    shifted_date = datetime.strptime(str(week_id) + '1', '%G%V%u') + timedelta(nb_weeks * 7)
    ret_week_id = get_week_id(shifted_date)
    return ret_week_id


def get_previous_n_week(week_id, nb_weeks):
    """
    Get previous week depending on nb_week
    @param nb_weeks should be positive
    """
    if nb_weeks < 0:
        raise ValueError('get_previous_n_week: nb_weeks argument should be positive')
    return get_shift_n_week(week_id, -nb_weeks)


def get_previous_week_id(week_id):
    """
    Get previous week: current week - 1
    """
    return get_previous_n_week(week_id, 1)


def get_next_n_week(week_id, nb_weeks):
    """
    Get next week depending on nb_week
    @param nb_weeks should be positive
    """
    if nb_weeks < 0:
        raise ValueError('get_next_n_week: nb_weeks argument should be positive')
    return get_shift_n_week(week_id, nb_weeks)


def get_next_week_id(week_id):
    """
    Get next week: current week + 1
    """
    return get_next_n_week(week_id, 1)


def get_time():
    return str(datetime.now())
