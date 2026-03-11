import json
import pandas as pd
import requests
from urllib import parse
import time
import matplotlib.pyplot as plt
import warnings
import plotly.express as px
import re
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Suppress only the single InsecureRequestWarning from urllib3 needed for this context
warnings.simplefilter('ignore', InsecureRequestWarning)

## datasets dictionary for UBS evidence lab
#  https://neo.ubs.com/api/evidence-lab/api-framework/catalogue/data-asset/v2
RETRIES = 3

class UBSEvidenceLab(object):

    def __init__(self, token, proxy=None):
        self.server = "https://neo.ubs.com/api/evidence-lab/api-framework/"
        self.proxy = proxy
        self.token = token

        
    def _http_get(self, url):
        return requests.get(
            url,
            headers={"Authorization": f"Bearer {self.token}"},
            proxies=self.proxy,
            # Set this to True or False depending on need
            verify=False
        )

    def get(self, endpoint):
        url = parse.urljoin(self.server, endpoint)
        for attempt in range(RETRIES+1):
            response = self._http_get(url)
            if response.status_code in [500, 503] and attempt < RETRIES:
                print(f"Request failed. Retrying {attempt + 1}/{RETRIES}...")
                time.sleep(5 ** (attempt + 1)) 
            else:
                break
        return self.handle_response(response)

    
    def _http_post(self, url, payload):
        return requests.post(
            url=url,
            headers={"Authorization": f"Bearer {self.token}"},
            json=payload,
            proxies=self.proxy,
            verify=False,
        )

    
    def post(self, endpoint, payload=None):
        url = parse.urljoin(self.server, endpoint)
        for attempt in range(RETRIES+1):
            response = self._http_post(url, payload)
            if response.status_code in [500, 503] and attempt < RETRIES:
                print(f"Request failed. Retrying {attempt + 1}/{RETRIES}...")
                time.sleep(5 ** (attempt + 1)) 
            else:
                break
        return self.handle_response(response)

    

    def handle_response(self, response):
        self.validate_response(response)
        data = response.json()
        return data

        
    @staticmethod
    def validate_response(response):
        if response.status_code == 401:
            raise Exception("Invalid credentials")
        if response.status_code == 404:
            raise Exception("API not found")
        if response.status_code >= 400:
            data = response.json if isinstance(response.json, dict) else response.json()
            raise Exception(data.get("message"))
        if response.status_code == 200 and 'HTML' in response.text:
            raise Exception("Invalid credentials")


def insta_plot(df, primaryExchangeTicker, plot_metrics):
    # Plotting
    df_plot = df
   
    # Create combined plotly line chart
    df_long = df_plot.melt(id_vars=['periodEndDate', 'compset', 'businessEntityDoingBusinessAsName'], value_vars=plot_metrics, 
                           var_name='metric', value_name='value')
    fig = px.line(df_long, x='periodEndDate', y='value', color='compset', facet_row='businessEntityDoingBusinessAsName', 
                  facet_col='metric', 
                  title=f'Weekly Instagram Metrics for {primaryExchangeTicker} by Brand and Compset')
    fig.update_yaxes(matches=None)
    fig.update_yaxes(showticklabels=True, col=2)
    fig.update_xaxes(showticklabels=True)
    num_entities = df['businessEntityDoingBusinessAsName'].nunique()
    fig.update_layout(height=400 * num_entities+100, width=1600)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.write_html(f'{primaryExchangeTicker}_instagram.html')
    print(f"Chart saved to {primaryExchangeTicker}_instagram.html")


def insta_plot_entity(df, primaryExchangeTicker, businessEntityDoingBusinessAsName, plot_metrics):
    """Plot Instagram metrics for a specific primaryExchangeTicker and businessEntityDoingBusinessAsName.

    Produces a multi-metric plot (faceted by metric) colored by compset and writes an HTML file.
    """
    # Filter by ticker and entity name
    df_plot = df.copy()
    if 'primaryExchangeTicker' in df_plot.columns:
        df_plot = df_plot[df_plot['primaryExchangeTicker'] == primaryExchangeTicker]
    df_plot = df_plot[df_plot['businessEntityDoingBusinessAsName'] == businessEntityDoingBusinessAsName]

    if df_plot.empty:
        print(f"No data found for {primaryExchangeTicker} / {businessEntityDoingBusinessAsName}")
        return

    # Prepare long format
    df_long = df_plot.melt(id_vars=['periodEndDate', 'compset'], value_vars=plot_metrics,
                           var_name='metric', value_name='value')

    # Create plotly line chart, faceted by metric
    fig = px.line(df_long, x='periodEndDate', y='value', color='compset',
                  facet_col='metric',
                  title=f'Weekly Instagram Metrics for {primaryExchangeTicker} - {businessEntityDoingBusinessAsName}')
    fig.update_yaxes(matches=None)
    fig.update_xaxes(showticklabels=True)
    fig.update_layout(height=400, width=1600)

    # Safe filename
    safe_entity = re.sub(r"\W+", "_", businessEntityDoingBusinessAsName)
    filename = f"{primaryExchangeTicker}_{safe_entity}_instagram.html"
    fig.write_html(filename)
    print(f"Chart saved to {filename}")


def plot_season(df, primaryExchangeTicker, metrics):
    entities = df['businessEntityDoingBusinessAsName'].unique()
    for entity in entities:
        df_entity = df[df['businessEntityDoingBusinessAsName'] == entity]
        # Find compset with highest followers
        compset_max = df_entity.groupby('compset')[metrics[0]].max().idxmax()
        df_plot = df_entity[df_entity['compset'] == compset_max].copy()
        df_plot['periodEndDate'] = pd.to_datetime(df_plot['periodEndDate'])
        df_plot['year'] = df_plot['periodEndDate'].dt.year
        df_plot['week'] = df_plot['periodEndDate'].dt.isocalendar().week
        fig = px.line(df_plot, x='week', y=metrics[0], color='year', 
                      title=f'Seasonality Chart of {metrics[0]} for {entity} ({primaryExchangeTicker})')
        fig.update_layout(height=400, width=600)
        fig.write_html(f'{primaryExchangeTicker}_{entity}_seasonality.html')
        print(f"Seasonality chart saved to {primaryExchangeTicker}_{entity}_seasonality.html")


# inputs
primaryExchangeTicker = "AEO"
start_date = "2023-01-01"
frequency = "Weekly" # Weekly, Monthly, Quarterly
plot_metrics = ["followers", "posts", "likes", "comments"] 
plot_metrics1 = ["interactions", "likesPerPost", "interactionsPerPost"] 
# followers, posts, pictures, videos, comments, likes, interactions, likesPerPost, interactionsPerPost, likesPer1000Followers,
# interactionsPer1000Followers, likesPerPostPer1000Followers, interactionsPerPostPer1000Followers

token = "eyJraWQiOiJhcmVzIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJhdWQiOiJQQ0M4YTlhYjZjMDgxNjU4MmYzMDE4MTY2Mjc2ZWVjMDUxNSIsInN1YiI6IkVMUC1BUEkiLCJpc3MiOiJFTEFQSSIsImV4cCI6MTgwMTU1Mzc5OCwiaWF0IjoxNzcwMDE3Nzk4LCJqdGkiOiJkZmE2NzgxOC1kYTkzLTQ3YjEtODUzMC1jMTRmMzY1YWJmMTkiLCJwcm9kdWN0cyI6WyJFTEFQSURhdGFTZXJ2aWNlcyJdfQ.XUkj1PZMgyxcuBPsB_Omhl-bZzmaP-K2XW2_674of8hN3ZHkHIdrPm_0iKi5w06NA3EgiKVYjTG6Kendd8vcxTgvf8VkHssdV-tKZr-ktJacS4JtXQJU5_zs6aZ_Ukl-JeCxLNUKUzYz8ZR1wQaAs4G85MMiqqwgwYIqQ9ki5h-Tc41xt0TReXqOOewzhdu_Pqxa-kEGSmFHRRhF9H8llUd5zQHqumcj13j9h_YJFJDYbYTMWmjSHzdzI8GvWLo637jt7m2JdzcvEf4_ORD7KawwUfmUYJhxjtpdYhxBfHxgmWzxGfBpG1BdehoI6s4Puj8k0S7Kg_4kph_qpXdraA"
# dataset end point
endpoint = "instagram/default/v2/data?dataAssetKey=10013"


if __name__ == '__main__':

    filter_dict = [
        {"filterType": ">", "field": "periodEndDate", "value": start_date},
        {"filterType": "=", "field": "period", "value": frequency},
        {"filterType": "=", "field": "primaryExchangeTicker", "value": primaryExchangeTicker},
    ]

    client = UBSEvidenceLab(token)
    df = pd.DataFrame()
    filter_dict = {"filters": filter_dict}
    
    try:
        while endpoint != "":
            data = client.post(endpoint=endpoint, payload=filter_dict)
            if 'results' in data and data['results']:
                df_data_page: pd.DataFrame = pd.json_normalize(data['results'])
                df = pd.concat([df, df_data_page], ignore_index=True)
            else:
                print("'results' key not found in API response.")
                break
            if "data" in endpoint and 'meta' in data and 'next' in data['meta'] and data['meta']['next']:
                endpoint = data['meta']['next']
                endpoint = endpoint.replace(client.server, '' )
            else:
                endpoint = ""
        
        # Export unique company names to Excel
        company_list = df.primaryExchangeTicker.unique()
        company_list_df = pd.DataFrame(company_list, columns=['primaryExchangeTicker'])
        print('Company list exported to company_list.xlsx')
        company_list_df.to_excel('company_list.xlsx', index=False)
        

        # Drop specified columns if present
        drop_cols = ['dataAssetKey', 'calculationType', 'domicileCountryName', 'leiId', 'primaryTickerIsin', 
                     'ultimateParentLegalEntityPrimaryExchangeName', 'ultimateParentLegalEntityName', 'primaryExchangeName', 
                     'primaryExchangeOperatingMic', 'ultimateParentLegalEntityPrimaryExchangeName',
                     'ultimateParentLegalEntityPrimaryExchangeOperatingMic', 'ultimateParentLegalEntityPrimaryExchangeTicker',
                     'ultimateParentLegalEntityPrimaryTickerIsin',]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])

        df = df.sort_values(by='periodEndDate')
        # print(df)
        print(f'DataFrame shape: {df.shape}')
        # print('Company has the following businessEntityDoingBusinessAsName values:')
        # print(df.businessEntityDoingBusinessAsName.unique())
        
        # # Export to Excel
        # df.to_excel(f'df_output_{primaryExchangeTicker}.xlsx', index=False)
        # print(f'Data exported to df_output_{primaryExchangeTicker}.xlsx')

        # # Generate plots
        insta_plot(df, primaryExchangeTicker, plot_metrics)
        # insta_plot(df, primaryExchangeTicker, plot_metrics1)

        ## Generate entity-specific plots
        # insta_plot_entity(df, primaryExchangeTicker, "Loewe", plot_metrics)

        ## Generate seasonality plots
        # plot_season(df, primaryExchangeTicker, plot_metrics)
        
    except Exception as e:
        print(f"Error: {e}")