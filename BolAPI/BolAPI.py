import csv
import time
import base64
import requests
import numpy as np
import pandas as pd

from typing import *
from io import BytesIO, StringIO
from PIL import Image

Response = Tuple[int, Type[requests.models.Response]]
Method = Literal['GET', 'POST', 'PUT']
Json = Dict
Date = str

class BolAPI:
    """ Super class for bol.com API
    
    Attributes
    ----------
    client_id : str        
        ID from client credentials 
    client_secret : str    
        Secret from client credentials 
    base_url : str         
        Initial part of the API URL for all requests
    headers : dict         
        API Headers containing:
            - Authentication credentials (Authorization)
            - Expected content type received in response (Accept)
            - Media type of current request body (Content-Type)
    """
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.bol.com"
        self.valid = None

    def get_access_token(self) -> None:
        """ Obtain access token

        Acquire a token from the authentication service using
        client credentials and set the token as an authorization header
        """
        credentials = base64.b64encode(bytes(f"{self.client_id}:{self.client_secret}", "utf-8")).decode("utf-8")
        credentials_header = {
            'Accept' : 'application/json',
            'Authorization': f'Basic {credentials}'
        }

        request = requests.post('https://login.bol.com/token?grant_type=client_credentials', headers=credentials_header)
        status = request.status_code
        response = request.json()

        if status == 200:
            self.valid = True
            access_token = response['access_token']
            bearer_str = f'Bearer {access_token}'
            self.headers.update({'Authorization' : bearer_str})
            return
        
        else:
            print(f'ERROR: failed to retrieve access token \n {request}')
            self.valid = False
            return

    def request(self, method: Method, url: str, data: Optional[dict] = None, headers: Optional[dict] = None) -> Response:
        """ Send a request to api.bol.com

        Configures the request and returns its status code with the response body.
        In case the access token has expired, new one will be requested.

        Parameters
        ----------
        method:  str   
            The HTTP Method for our request ['GET', 'POST', 'PUT']
        url : str    
            Endpoint location, appends to self.base_url
        data : dict  
            Optional; Query Parameters (default is None)
        headers : dict  
            Optional; Header Parameters (default is self.headers)

        Returns
        -------
        status_code : int  
            Status code of request
        request : Any           
            Request response
        """
        # Update headers if necessary
        if headers:
            headers_ = self.headers.copy()
            headers_.update(headers)
            headers = headers_
        else:
            headers = self.headers
        
        # Set the request url
        request_url = f'{self.base_url}/{url}'

        # Make request
        if method == 'GET':
            request = requests.get(request_url, params=data, headers=headers)
        elif method == 'POST':
            request = requests.post(request_url, headers=headers, json=data)
        elif method == 'PUT':
            request = requests.put(request_url, params=data, headers=headers)
        
        # Check if request has been denied due to token expiration
        status_code = request.status_code
        if status_code == 401:
            print('Token expired, acquiring new one...')
            self.get_access_token()
            return self.request(method, url, data, headers)

        if status_code == 429:
            print('Too many requests, retrying after 1550 seconds')
            time.sleep(1550)
            return  self.request(method, url, data, headers)
        else:
            return status_code, request
    
    def get(self, url: str, data: Optional[dict] = None, headers: Optional[dict] = None) -> Response:
        status, response = self.request('GET', url, data, headers)
        return status, response
    
    def post(self, url: str, data: Optional[dict] = None, headers: Optional[dict] = None) -> Response:
        status, response = self.request('POST', url, data, headers)
        return status, response
    
    def put(self, url: str, data: Optional[dict] = None, headers: Optional[dict] = None) -> Response:
        status, response = self.request('PUT', url, data, headers)
        return status, response


class BolRetailerAPI(BolAPI):
    """ Initializes API connection with bol Retailer API
    """

    def __init__(self, client_id: str, client_secret: str) -> None:
        super().__init__(client_id, client_secret)   
        self.headers = {
            'Accept' : 'application/vnd.retailer.v9+json',
            'Content-Type' : 'application/vnd.retailer.v9+json'
        }
        self.get_access_token()

    def request_invoice_specification(self, invoice_id: str) -> pd.DataFrame:
        """ Retrieve invoice specification by invoice ID

        Parameters
        ----------
        invoice_id : str  
            ID of the specified invoice

        Returns
        -------
        json               
            Json-format dict containing all details in invoice specification
        """
        headers = {
            'Accept' : 'application/vnd.retailer.v10+openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Type' : 'None'
        }
        endpoint_url = f'retailer/invoices/{invoice_id}/specification'
        status, response = self.get(endpoint_url, headers=headers)
        
        if status == 200:
            df = pd.read_excel(response.content)
            df.rename(columns=df.iloc[6], inplace=True) # Set row 6 as column headers
            df.drop(np.arange(0,7), inplace=True) # Remove first 7 rows that aren't part of table
            df.reset_index(drop=True, inplace=True) # Reset the indexing
            return df
            
        else: print(f'ERROR: failed to retrieve invoice specification \n{response.text}')
        
    def request_all_offers(self) -> csv:
        """ Retrieve information on all offers

        Prepares a offer export CSV file, checks when 
        the export is ready and retrieves the file.

        Data contains following columns: 
            - offerID
            - ean 
            - conditionName
            - conditionCategory
            - conditionComment
            - bundlePricesPrice
            - fulfilmentDeliveryCode
            - stockAmount
            - onHoldByRetailer
            - fulfilmentType
            - mutationDateTime
            - referenceCode
            - correctedStock

        Returns
        -------
        csv
            Decoded csv data containing details of all offers
        """
        # Prepare offer export
        data = {"format": "CSV"}
        endpoint_url = 'retailer/offers/export'
        
        status, response = self.post(endpoint_url, data=data)

        if status == 202:
            # Check status of offer export
            process_id = response.json()['processStatusId']
            endpoint_url = f'shared/process-status/{process_id}'
            _, process = self.get(endpoint_url)
            
            while process.json()['status'] == 'PENDING':
                time.sleep(60) # Wait 60 seconds
                _, process = self.get(endpoint_url)
            
            if process.json()['status'] == 'SUCCESS':
                report_id = process.json()['entityId']
                endpoint_url = f'retailer/offers/export/{report_id}'
                headers = {
                    'Accept' : 'application/vnd.retailer.v9+csv',
                    'Content-Type' : 'application/x-www-form-urlencoded'
                }
                status, response = self.get(endpoint_url, headers=headers)
                
                if status == 200:
                    return StringIO(response.content.decode('utf-8'))
                
                else: print(f'ERROR: failed to retrieve offer export \n{response.text}')
            else: print(f'ERROR: failed to process offer export \n{process}')
        else: 
            print(f'ERROR: failed to request offer export \n{response.text}')
            return response

    def request_offer(self, offer_id: str) -> Json:
        """ Retrieve an offer by its ID

        Obtains information of listed offer, 
        useful for retrieving product title

        Parameters
        ----------
        offer_id : str
            The ID of the offer

        Returns
        -------
        dict    
            Json-formatted dictionary containing offer details
        """

        endpoint_url = f'retailer/offers/{offer_id}'
        status, response = self.get(endpoint_url)
        if status == 200:
            return response.json()
        else:
            print(f'ERROR: failed to retrieve offer \n{response.text}')

    def request_offer_forecast(self, offer_id: str, weeks: int) -> Tuple[float, float]:
        """ Retrieve sales forecast of an offer

        Get estimated the sales expectations on the total bol.com platform 
        for the requested number of weeks ahead.
        
        In case product is sold less than 2 times in past 28 days, a range 
        will be shown. Otherwise minimum==maximum.

        Parameters
        ----------
        offer_id : str 
            The ID of the offer
        weeks : int   
            The requested no. weeks ahead
        
        Returns
        -------
        minimum : float 
            The minimum no. expected sales
        maximum : float
            The maximum no. expected sales
        """

        data = {
            'offer-id': offer_id,
            'weeks-ahead': weeks
        }
        endpoint_url = 'retailer/insights/sales-forecast'

        status, response = self.get(endpoint_url, data=data)
        
        if status == 200:
            minimum = response.json()['total']['minimum']
            maximum = response.json()["total"]["maximum"]
            
            return minimum, maximum

        else: print(f'ERROR: failed to retrieve offer sales forecast \n{response.text}')

    def request_offer_insights(self, offer_id: str, 
                           name: Literal['PRODUCT_VISITS', 'BUY_BOX_PERCENTAGE'], 
                           period: Literal['DAY', 'WEEK', 'MONTH'], 
                           no_periods: int, 
                           country: Optional[Literal['NL', 'BE']] = 'NL') -> Any:
        """ Retrieve offer insights from past

        Get the no. product visits or buy-box percentage of an offer
        within a certain period. Format to two lists for pd.DataTable usage.

        Parameters
        ----------
        offer_id : str     
            The ID of the offer
        name : str         
            The name of the requested offer insight ['PRODUCT_VISITS', 'BUY_BOX_PERCENTAGE']
        period : str       
            The time unit in which the offer insights are grouped ['DAY', 'WEEK', 'MONTH']
        no_periods : int   
            The no. periods for which the offer insights are requested back in time.
            Max values are 730, 104 and 24 for DAY, WEEK and MONTH respectively
        country : str      
            The country code of requested offer insight ['NL', 'BE']

        Returns
        -------
        dates : list       
            List of strings, contains the dates
        values : list     
            List of floats, contains measured values
        """

        data = {
            'offer-id' : offer_id,
            'period' : period,
            'number-of-periods' : no_periods,
            'name' : name
        }
        endpoint_url = 'retailer/insights/offer'
        
        status, response = self.get(endpoint_url, data=data)
        
        if status == 200:
            response = response.json()
            
            dates = [] # The dates corresponding to measured value
            values = [] # Measured values per date

            for i in response['offerInsights'][0]['periods']:
                date = i['period']
                countries = i['countries']

                if period == 'DAY':
                    date_str = f"{date['day']}-{date['month']}-{date['year']}"
                elif period == 'MONTH':
                    date_str = f"{date['month']}-{date['year']}"
                elif period == 'YEAR':
                    date_str = f"{date['year']}"
                
                dates.append(date_str)

                for c in countries:
                    if c['countryCode'] == country:
                        values.append(c['value'])
            
            if no_periods == 1:
                return values[0]
            else:
                if name == 'PRODUCT_VISITS':  
                    return {'Date': dates, 'Visits': values}
                else: 
                    return {'Date': dates, 'Buy-Box %': values}

        else: print(f'ERROR: failed to retrieve offer insights \n{response.text}')

    def request_product_image(self, ean: str) -> Image:
        """ Retrieve primary image of product by EAN

        Obtain image url of the primary product image,
        request the image and save into variable

        Parameters
        ----------
        ean : str  
            EAN of product
        
        Returns
        -------
        img : PIL.Image
            Image of product
        """
        endpoint_url = f'retailer/products/{ean}/assets?usage=PRIMARY'
        status, response = self.get(endpoint_url)

        if status == 200:
            img_url = response.json()['assets'][0]['variants'][0]['url']
            img = Image.open(requests.get(img_url, stream=True).raw)
            
            return img
        
        else: print(f'ERROR: failed to retrieve product\'s primary image \n{response.text}')

    def request_product_ratings(self, ean: str) -> Dict[int, int]:
        """ Retrieve product ratings by EAN

        Obtain the count of each rating for given product

        Parameters
        ----------
        ean : str  
            EAN of product

        Returns
        -------
        dict
            Dictionary containing the ratings and corresponding counts
        """

        dict_data = {
            'Rating' : [],
            'Count' : []
        }

        endpoint_url = f'retailer/products/{ean}/raings'
        status, response = self.get(endpoint_url)

        if status == 200:
            response = response.json()
            
            for i in response['ratings']:
                dict_data['Rating'].append(i['rating'])
                dict_data['Count'].append(i['count'])

        return dict_data
    
class BolAdvertisingAPI(BolAPI):
    """ Initializes API connection with bol Advertising API v11 ALPHA

    Methods
    -------
    request_campaigns_report(start_date, end_date)
        Obtain performance results of all campaign for requested period
    """

    def __init__(self, client_id: str, client_secret: str):
        super().__init__(client_id, client_secret)
        self.headers = {
            'Accept' : 'application/vnd.advertiser.v11+json',
            'Content-Type' : 'application/vnd.advertiser.v11+json'
        }
        self.get_access_token()
    

    def request_bulk_report(self, entity_type: str, start_date: str, end_date: str):
        endpoint_url = f'advertiser/sponsored-products/reporting/bulk-reports?entity-type={entity_type}&start-date={start_date}&end-date={end_date}'
        status, response = self.post(endpoint_url)

        if status == 202:
            # Check status of offer export
            process_id = response.json()['processStatusId']
            endpoint_url = f'shared/process-status/{process_id}'
            _, process = self.get(endpoint_url)
            
            while process.json()['status'] == 'PENDING':
                time.sleep(60) # Wait 60 seconds
                _, process = self.get(endpoint_url)
            
            if process.json()['status'] == 'SUCCESS':
                report_id = process.json()['entityId']
                endpoint_url = f'advertiser/sponsored-products/reporting/bulk-reports/{report_id}'
                status, response = self.get(endpoint_url)
                if status == 200:
                    download_url = response.json()['url']
                    report = requests.get(download_url)

                    return pd.read_csv(StringIO(report.content.decode('utf-8')))
                
                else: print(f'ERROR: failed to retrieve campaign performance \n{response.text}')

        else:
            print(f'ERROR: failed to request campaign performance export \n{response.text}')


class BolAdvertisingAPIv10(BolAPI):
    """ Initializes API connection with bol Advertising API v10

    Methods
    -------
    request_campaigns_report(start_date, end_date)
        Obtain performance results of all campaign for requested period
    """

    def __init__(self, client_id: str, client_secret: str):
        super().__init__(client_id, client_secret)
        self.headers = {
            'Accept' : 'application/vnd.advertiser.v10+json',
            'Content-Type' : 'application/vnd.advertiser.v10+json'
        }
        self.get_access_token()
    

    def request_campaigns_report(self, start_date: str, end_date: str):
        """ Request and retrieve campaign performance report for requested period

        The specified date range should be seven days apart.
        start_date and end_date must be within the past one year from the current 
        date with end-date being at least one day after the start-date.

        The following performance metrics are provided:
            - date
            - campaignId
            - campaignName
            - impressions
                no. times sponsored product is shown
            - clicks
                no. times sponsored product is clicked
            - ctr (click-through-rate) 
                clicks/impressions
            - conversions 
                no. sales through sponsored products
            - conversionRate
                avg conversions per click
            - sales
            - conversionsOtherEan
            - salesOtherEan
            - spent
            - cpc (cost per click)
            - acos (Advertising Cost of Sale)
                advertising costs calculated against revenue in %
            - roas (Return On Ad-Spent)
                revenue generated from ad calculated against advertising costs in %

        Parameters
        ----------
        start_date : str   
            The start date for the reporting period. 
            Period start date in ISO 8601 standard.
        end_date : str   
            The end date for the reporting period. 
            This will be one day after the last full day that is included in the reporting. 
            Period end date in ISO 8601 standard.

        Returns
        -------
            pd.DataFrame                
                DataFrame with performance results of all campaigns for requested period.
        """
        endpoint_url = f'advertiser/sponsored-products/campaign-performance/reports?start-date={start_date}&end-date={end_date}'
        status, response = self.post(endpoint_url)

        if status == 202:
            # Check status of offer export
            process_id = response.json()['processStatusId']
            endpoint_url = f'shared/process-status/{process_id}'
            _, process = self.get(endpoint_url)
            
            while process.json()['status'] == 'PENDING':
                time.sleep(60) # Wait 60 seconds
                _, process = self.get(endpoint_url)
            
            if process.json()['status'] == 'SUCCESS':
                report_id = process.json()['entityId']
                endpoint_url = f'advertiser/sponsored-products/campaign-performance/reports/{report_id}'
                status, response = self.get(endpoint_url)
                if status == 200:
                    download_url = response.json()['url']
                    report = requests.get(download_url)

                    return pd.read_csv(StringIO(report.content.decode('utf-8')))
                
                else: print(f'ERROR: failed to retrieve campaign performance \n{response.text}')

        else:
            print(f'ERROR: failed to request campaign performance export \n{response.text}')
        


class BolAdvertisingAPIv9(BolAPI):
    def __init__(self, client_id: str, client_secret: str) -> None:
        super().__init__(client_id, client_secret)
        self.headers = {
            'Accept' : 'application/vnd.advertiser.v9+json',
            'Content-Type' : 'application/vnd.advertiser.v9+json'
        }
        self.get_access_token()

    def request_adgroups(self, campaign_id: str):
        endpoint_url = f'advertiser/sponsored-products/ad-groups?campaign-id={campaign_id}'
        status, response = self.get(endpoint_url)

        if status == 200:
            return response.json()
        else: print(f'ERROR: failed to retrieve campaign ad-groups \n{response.text}')

    def request_targetproducts(self, adgroup_id: str):
        endpoint_url = f'advertiser/sponsored-products/target-products?ad-group-id={adgroup_id}'
        status, response = self.get(endpoint_url)

        if status == 200:
            return response.json()
        else: print(f'ERROR: failed to retrieve ad-group target-products \n{response.text}')