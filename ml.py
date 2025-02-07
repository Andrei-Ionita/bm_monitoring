from prophet import Prophet
import pandas as pd

def forecast_afrr_mfrr(df, forecast_period=5):
    # Prepare data for Prophet
    df_afrr = df[["Time Period", "aFRR Value"]].rename(columns={"Time Period": "ds", "aFRR Value": "y"})
    df_mfrr = df[["Time Period", "mFRR Value"]].rename(columns={"Time Period": "ds", "mFRR Value": "y"})

    # Initialize and fit the models
    afrr_model = Prophet()
    mfrr_model = Prophet()

    afrr_model.fit(df_afrr)
    mfrr_model.fit(df_mfrr)

    # Forecast the next 'forecast_period' time points
    future_afrr = afrr_model.make_future_dataframe(periods=forecast_period, freq='H')
    future_mfrr = mfrr_model.make_future_dataframe(periods=forecast_period, freq='H')

    forecast_afrr = afrr_model.predict(future_afrr)
    forecast_mfrr = mfrr_model.predict(future_mfrr)

    return forecast_afrr[['ds', 'yhat']], forecast_mfrr[['ds', 'yhat']]
