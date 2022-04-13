from datetime import datetime
from functools import partialmethod
from util import import_formatted_warnings
from util import change_values
import pandas as pd
import fredapi as Fred
import warnings
import numpy as np
import math

formatted_warnings = import_formatted_warnings()

#24 functions
def fit_data_to_date_range(data, date_from, frequency="M"):
    data = data[(data.index >= date_from) & (data.index <= datetime.datetime.now())]
    index = data.index
    if index[0] != pd.Timestamp(date_from):
        full_idx = pd.date_range(date_from, data.index[-1], freq=frequency)
        diff_idx = full_idx.difference(data.index)
        data = pd.concat([pd.DataFrame(index=diff_idx), data], axis=1)
    return data
def make_date_from_for_rate_data(self, date_from, shift_period=1):
    # return을 연산하면 첫째값이 nan이 된다. 이를 방지하기 위해서 shift해주는 period만큼 이전 데이터를 요청한다.
    return (pd.Timestamp(date_from) - pd.DateOffset(months=shift_period + 1)).strftime('%Y-%m-%d')
def get_wilshire5000_price_index_momentum(date_from='1986-12-31', date_to='9999-12-31', periods=1):
    req_date_from = (pd.Timestamp(date_from) - pd.DateOffset(months=periods)).strftime('%Y-%m-%d')
    data = Fred.wilshire5000_price_index(date_from=req_date_from, date_to=date_to).pct_change(periods, fill_method=None)

    return data
def get_fed_target_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: FOMC Press Release
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """

    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    data = Fred.get_concatenated_series_all_releases('DFEDTAR', 'DFEDTARU')

    return data
def get_dff(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.15 Selected Interest Rates
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)4
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('DFF', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_m1_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M', convert_to_rate=True):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.6 Money Stock, Liquid Assets, and Debt Measures / H.6 Money Stock and Debt Measures / H.6 Money Stock Measures
    Release Frequency: Weekly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from
    data = Fred.get_time_series_data('M1', date_from, date_to, frequency)

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_m2_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.6 Money Stock, Liquid Assets, and Debt Measures / H.6 Money Stock and Debt Measures / H.6 Money Stock Measures
    Release Frequency: Weekly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('M2', date_from, date_to, frequency)

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_mzm_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M', convert_to_rate=True):
    """
    Seasonally adjusted Velocity of MZM Money Stock
    Source: Federal Reserve Bank of St. Louis
    Release: Money Zero Maturity (MZM)
    Release Frequency: Weekly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    req_date_from = date_from

    data = Fred.get_time_series_data('MZM', date_from, date_to, frequency, report_start='1998-11-20')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_moodys_baa_corporate_bond_yield(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US) / Moody’s
    Release: H.15 Selected Interest Rates / Moody's Daily Corporate Bond Yield Averages
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from
    data = Fred.get_time_series_data('DBAA', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_gdp(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: U.S. Bureau of Economic Analysis
    Release: Gross Domestic Product
    Release Frequency: Quarterly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('GDPC1', date_from, date_to, frequency, report_start='1992-12-22')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_treasury_3m(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.15 Selected Interest Rates
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('DTB3', date_from, date_to, frequency, report_start='2006-02-07')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_treasury_2y(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.15 Selected Interest Rates
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('DGS2', date_from, date_to, frequency, report_start='2006-02-07')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_treasury_5y(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.15 Selected Interest Rates
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('DGS5', date_from, date_to, frequency, report_start='2006-02-07')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_treasury_10y(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.15 Selected Interest Rates
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('DGS10', date_from, date_to, frequency, report_start='2006-02-07')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_treasury_30y(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: H.15 Selected Interest Rates
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('DGS30', date_from, date_to, frequency, report_start='2006-03-14')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_traded_weighted_dollar_index(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Board of Governors of the Federal Reserve System (US)
    Release: G.5 Foreign Exchange Rates
    Release Frequency: Monthly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_concatenated_series_all_releases('DTWEXB', 'DTWEXBGS')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_t10y2y(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: Federal Reserve Bank of St. Louis
    Release: Interest Rate Spreads
    Release Frequency: Daily
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('T10Y2Y', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_pce(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: U.S. Bureau of Economic Analysis
    Release: Personal Income and Outlays
    Release Frequency: Monthly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('PCE', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)

    return data
def get_us_unrate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Source: U.S. Bureau of Labor Statistics
    Release: Employment Situation
    Release Frequency: Monthly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    formatted_warnings.warn('[FredAPI] FRED API 로 변경되었음')
    req_date_from = date_from

    data = Fred.get_time_series_data('UNRATE', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)

    return data
def get_us_home_price_index(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Seasonally adjusted U.S. National home price index
    Source: Standard and Poor's / S&P Dow Jones Indices LLC
    Release: S&P/Case-Shiller Home Price Indices
    Release Frequency: Monthly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    req_date_from = date_from

    data = Fred.get_time_series_data('CSUSHPISA', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)

    return data
def get_cpi(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Seasonally adjusted Consumer Price Index for All Urban Consumers: All Items in U.S. City Average.
    Source: U.S. Bureau of Labor Statistics
    Release: Consumer Price Index
    Release Frequency: Monthly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    req_date_from = date_from

    data = Fred.get_time_series_data('CPIAUCSL', date_from, date_to, frequency,
                                     reliable_start='1989-01-01')

    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_m1_velocity(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Seasonally adjusted Velocity of M1 Money Stock
    Source: Federal Reserve Bank of St. Louis
    Release: Money Velocity
    Release Frequency: Quarterly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    req_date_from = date_from

    data = Fred.get_time_series_data('M1V', date_from, date_to, frequency)
    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_us_m2_velocity(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Seasonally adjusted Velocity of M2 Money Stock
    Source: Federal Reserve Bank of St. Louis
    Release: Money Velocity
    Release Frequency: Quarterly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    req_date_from = date_from
    data = Fred.get_time_series_data('M2V', date_from, date_to, frequency)

    data = fit_data_to_date_range(data, req_date_from)

    return data
def get_us_mzm_velocity(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    """
    Seasonally adjusted Velocity of MZM Money Stock
    Source: Federal Reserve Bank of St. Louis
    Release: Money Velocity
    Release Frequency: Quarterly
    Via Archival FRED(https://alfred.stlouisfed.org/)
    """
    req_date_from = date_from
    data = Fred.get_time_series_data('MZMV', date_from, date_to, frequency, convert_to_rate)
    data = fit_data_to_date_range(data, req_date_from)

    return data
## 매크로 월별 주기 추가
# 월 데이터이므로 마지막 월 데이터가 아직 생성되지 않았을 수 있음. pretend 할 때에 주의 필요


#21 functions
def get_core_cpi_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.core_cpi(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_core_pce_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.core_pce(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_core_ppi_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.core_ppi(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_cpi_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.cpi(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_home_price_index_rate(date_from='1986-12-31', date_to='9999-12-31', frequency="M"):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.home_price_index(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_industrial_production_index_rate(date_from='1986-12-31', date_to='9999-12-31', frequency="M"):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.industrial_production_index(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_pce_rate(date_from='1986-12-31', date_to='9999-12-31', frequency="M"):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.pce(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_ppi_rate(date_from='1986-12-31', date_to='9999-12-31', frequency="M"):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.ppi(date_from=date_from, date_to=date_to, frequency=frequency)
    rate_df = rate_df / rate_df.shift(1) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from, frequency)

    return rate_df
def get_trimmed_mean_pce_rate(change_to, frequency="M"):
    rate_df = Fred.trimmed_mean_pce_rate(frequency=frequency)
    rate_df = change_values(rate_df, change_to)

    return rate_df
def get_unemployment_rate(frequency="M"):  # change_to
    rate_df = Fred.unemployment_rate(frequency=frequency)

    return rate_df
def get_wti_price_rate(date_from='1986-12-31', date_to='9999-12-31', frequency="M"):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    growth_df = Fred.get_time_series_data('WTISPLC', date_from=date_from, date_to=date_to, frequency=frequency)
    growth_df = growth_df / growth_df.shift(1) - 1.
    growth_df = fit_data_to_date_range(growth_df, req_date_from, frequency)

    return growth_df
def get_corporate_profits_after_tax_growth(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    growth_df = Fred.corporate_profits_after_tax(date_from=date_from, date_to=date_to)
    growth_df = growth_df / growth_df.shift(3) - 1.
    growth_df = fit_data_to_date_range(growth_df, req_date_from)

    return growth_df
def get_federal_debt_to_gdp_growth(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    growth_df = Fred.federal_debt_to_gdp(date_from=date_from, date_to=date_to)
    growth_df = growth_df / growth_df.shift(3) - 1.
    growth_df = fit_data_to_date_range(growth_df, req_date_from)

    return growth_df
def get_federal_government_current_tax_receipts_growth(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    growth_df = Fred.federal_government_current_tax_receipts(date_from=date_from, date_to=date_to)
    growth_df = growth_df / growth_df.shift(3) - 1.
    growth_df = fit_data_to_date_range(growth_df, req_date_from)

    return growth_df
def get_gross_government_saving_growth(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    growth_df = Fred.gross_government_saving(date_from=date_from, date_to=date_to)
    growth_df = growth_df / growth_df.shift(3) - 1.
    growth_df = fit_data_to_date_range(growth_df, req_date_from)

    return growth_df
def get_gross_private_saving_growth(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    growth_df = Fred.gross_private_saving(date_from=date_from, date_to=date_to)
    growth_df = growth_df / growth_df.shift(3) - 1.
    growth_df = fit_data_to_date_range(growth_df, req_date_from)

    return growth_df
def get_m1_velocity_rate(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    rate_df = Fred.m1_velocity(date_from=date_from, date_to=date_to)
    rate_df = rate_df / rate_df.shift(3) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from)

    return rate_df
def get_m2_velocity_rate(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    rate_df = Fred.m2_velocity(date_from=date_from, date_to=date_to)
    rate_df = rate_df / rate_df.shift(3) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from)

    return rate_df
def get_mzm_velocity_rate(date_from='1986-12-31', date_to='9999-12-31'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from, shift_period=3)
    rate_df = Fred.mzm_velocity(date_from=date_from, date_to=date_to)
    rate_df = rate_df / rate_df.shift(3) - 1.
    rate_df = fit_data_to_date_range(rate_df, req_date_from)

    return rate_df
def get_retail_money_funds_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.retail_money_funds(date_from=date_from, date_to=date_to, frequency=frequency)
    data = rate_df / rate_df.shift(1) - 1.
    data = fit_data_to_date_range(data, req_date_from)
    return data
def get_institutional_money_funds_rate(date_from='1986-12-31', date_to='9999-12-31', frequency='M'):
    req_date_from = date_from
    date_from = make_date_from_for_rate_data(date_from)
    rate_df = Fred.institutional_money_funds(date_from=date_from, date_to=date_to, frequency=frequency)
    data = rate_df / rate_df.shift(1) - 1.
    data = fit_data_to_date_range(data, req_date_from)
    return data
