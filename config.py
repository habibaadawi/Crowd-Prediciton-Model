import os
import requests
import wget
import kaggle
#import geopandas as gpd
#from geopy.distance import geodesic
import pandas as pd 
import numpy as np 
import duckdb 
import sys
import os
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
from sklearn.ensemble import IsolationForest
#from google.transit import gtfs_realtime_pb2, FeedMessage
import re 
import requests
import datetime
import time 
globals().update({
    'pd': pd,
    'np': np
})