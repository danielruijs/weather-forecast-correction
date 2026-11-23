# **Project proposal \- Group 20**

## **Problem description**

We will predict the error of weather forecasts at single points in Sweden, so that a raw forecast can be corrected and given a confidence interval. For a point (latitude, longitude) and a time, the model takes the forecast and recent weather data and outputs the expected error, observed minus forecast, in air temperature and precipitation 1, 6, 12 and 24 hours ahead.

## **Tools**

Python with polars, NumPy, scikit-learn and XGBoost; the `tabpfn` package with the [TabPFN-3.5](https://huggingface.co/Prior-Labs/tabpfn_3_5) weights; PyTorch; matplotlib; Modal and GitHub.

## **Data**

* **Observations (ground truth and station context):** Historical air temperature and precipitation observations from SMHI.  
* **Forecasts and other weather data:** Historical ECMWF IFS HRES forecasts from Open-Meteo's Single Runs API. We use this as the forecast source since no suitable open archive of past SMHI forecasts was identified.

**Split (chronological):** the test set is the most recent 365 days, the calibration set the 365 days before that, and the training set the rest. 

## **Methodology and Algorithm**

Each model is a regressor that predicts the forecast error in temperature (°C) and precipitation (mm/h) at the four future time points.

**Input.** The forecast for the target time, the current and previous 24 or 72 hours of observations, with coordinates and time.

**Models.**

* Baselines: rolling bias correction and XGBoost.  
* TabPFN-3.5: pretrained tabular model.  
* LSTM and Transformer (PyTorch): Trained from scratch to capture temporal patterns in recent weather observations and forecasts.  
* WeatherNext 2: adaptation of a pretrained weather forecasting model for forecast error prediction.

**Continuous training.** Trained models are updated in time order and saved as checkpoints after each year of data, each evaluated on the test set.

**Confidence intervals and evaluation.** Split conformal prediction on the calibration set gives a 90% interval per variable and future time point. We report MAE and RMSE against the actual error on the test set, plus interval coverage and width.