import time
import os
import urllib.parse
from typing import Optional, Dict, Any, List
import pandas as pd 
from requests import Request, Session, Response
import hmac
#from ciso8601 import parse_datetime

class FtxClient:
    def __init__(self, base_url=None, api_key=None, api_secret=None, subaccount_name=None) -> None:
        self._session = Session()
        self._base_url = 'https://ftx.com/api/'
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, json=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._base_url + path, **kwargs)
        if self._api_key:
            self._sign_request(request)
        response = self._session.send(request.prepare())
        
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self._subaccount_name)

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    
    #
    # Authentication required methods
    #

    def authentication_required(fn):
        """Annotation for methods that require auth."""
        def wrapped(self, *args, **kwargs):
            if not self._api_key:
                raise TypeError("You must be authenticated to use this method")
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    @authentication_required
    def get_account_info(self) -> dict:
        return self._get(f'account')
    
    @authentication_required
    def get_leverage(self) -> dict:
        account = self._get(f'account')
        leverage = account['totalPositionSize']/account['totalAccountValue']
        return leverage
    
    @authentication_required
    def get_open_orders(self, market: str = None) -> List[dict]:
        return self._get(f'orders', {'market': market})
    
    @authentication_required
    def get_order_history(self, market: str = None, side: str = None, order_type: str = None, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get(f'orders/history', {'market': market, 'side': side, 'orderType': order_type, 'start_time': start_time, 'end_time': end_time})
   
    @authentication_required
    def get_conditional_order_history(self, market: str = None, side: str = None, type: str = None, order_type: str = None, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get(f'conditional_orders/history', {'market': market, 'side': side, 'type': type, 'orderType': order_type, 'start_time': start_time, 'end_time': end_time})
    
    @authentication_required
    def get_fills(self, market: str = None, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get(f'fills', {'market': market, 'start_time': start_time, 'end_time': end_time})

    @authentication_required
    def modify_order(
        self, existing_order_id: Optional[str] = None,
        existing_client_order_id: Optional[str] = None, price: Optional[float] = None,
        size: Optional[float] = None, client_order_id: Optional[str] = None,
    ) -> dict:
        assert (existing_order_id is None) ^ (existing_client_order_id is None), \
            'Must supply exactly one ID for the order to modify'
        assert (price is None) or (size is None), 'Must modify price or size of order'
        path = f'orders/{existing_order_id}/modify' if existing_order_id is not None else \
            f'orders/by_client_id/{existing_client_order_id}/modify'
        return self._post(path, {
            **({'size': size} if size is not None else {}),
            **({'price': price} if price is not None else {}),
            ** ({'clientId': client_order_id} if client_order_id is not None else {}),
        })

    @authentication_required
    def get_conditional_orders(self, market: str = None) -> List[dict]:
        return self._get(f'conditional_orders', {'market': market})

    
    @authentication_required
    def place_order(self, market: str, side: str, price: float, size: float, type: str = 'limit',
                    reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                    client_id: str = None) -> dict:
        return self._post(f'orders', {'market': market,
                                     'side': side,
                                     'price': price,
                                     'size': size,
                                     'type': type,
                                     'reduceOnly': reduce_only,
                                     'ioc': ioc,
                                     'postOnly': post_only,
                                     'clientId': client_id,
                                     })

    
    @authentication_required
    def get_order_status(self, order_id: int) -> dict:
        return self._get(f'orders/{order_id}')
    
    @authentication_required
    def place_conditional_order(
        self, market: str, side: str, size: float, type: str = 'stop',
        limit_price: float = None, reduce_only: bool = False, cancel: bool = True,
        trigger_price: float = None, trail_value: float = None
    ) -> dict:
        """
        To send a Stop Market order, set type='stop' and supply a trigger_price
        To send a Stop Limit order, also supply a limit_price
        To send a Take Profit Market order, set type='trailing_stop' and supply a trigger_price
        To send a Trailing Stop order, set type='trailing_stop' and supply a trail_value
        """
        assert type in ('stop', 'take_profit', 'trailing_stop')
        assert type not in ('stop', 'take_profit') or trigger_price is not None, \
            'Need trigger prices for stop losses and take profits'
        assert type not in ('trailing_stop',) or (trigger_price is None and trail_value is not None), \
            'Trailing stops need a trail value and cannot take a trigger price'

        return self._post(f'conditional_orders',
                          {'market': market, 'side': side, 'triggerPrice': trigger_price,
                           'size': size, 'reduceOnly': reduce_only, 'type': 'stop',
                           'cancelLimitOnTrigger': cancel, 'orderPrice': limit_price})

    @authentication_required
    def cancel_order(self, order_id: str) -> dict:
        return self._delete(f'orders/{order_id}')

    @authentication_required
    def cancel_orders(self, market_name: str = None, conditional_orders: bool = False,
                      limit_orders: bool = False) -> dict:
        return self._delete(f'orders', {'market': market_name,
                                        'conditionalOrdersOnly': conditional_orders,
                                        'limitOrdersOnly': limit_orders,
                                        })

    @authentication_required
    def get_balances(self) -> List[dict]:
        return self._get(f'wallet/balances')
    
    @authentication_required
    def get_balance(self, name: str) -> dict:
        return next(filter(lambda x: x['coin'] == name, self.get_balances()), None)
    
    @authentication_required
    def get_deposit_address(self, ticker: str) -> dict:
        return self._get(f'wallet/deposit_address/{ticker}')

    @authentication_required
    def get_positions(self, show_avg_price: bool = False) -> List[dict]:
        return self._get(f'positions', {'showAvgPrice': show_avg_price})

    @authentication_required
    def get_position(self, name: str, show_avg_price: bool = False) -> dict:
        return next(filter(lambda x: x['future'] == name, self.get_positions(show_avg_price)), None)
    
    @authentication_required
    def set_leverage(self, leverage):
        return self._post(f'account/leverage', {'leverage': leverage})

    @authentication_required
    def get_subaccounts(self) -> List[dict]:
        return self._get(f'subaccounts')

    @authentication_required
    def create_subaccounts(self, nickname) -> List[dict]:
        return self._post(f'subaccounts', {'nickname': nickname})

    @authentication_required
    def delete_subaccounts(self, nickname) -> List[dict]:
        return self._delete(f'subaccounts', {'nickname': nickname})

    @authentication_required
    def get_subaccounts_balance(self, nickname) -> List[dict]:
        return self._get(f'subaccounts/{nickname}/balances', {'nickname': nickname})

    @authentication_required
    def request_quote(self, fromCoin, toCoin , size) -> List[dict]:
        return self._post(f'otc/quotes', {'fromCoin': fromCoin, 'toCoin': toCoin, 'size': size})

    #
    # Public methods
    #

    def get_futures(self) -> List[dict]:
        return self._get(f'futures')

    def get_future(self, future_name: str) -> dict:
        return self._get(f'futures/{future_name}')

    def get_markets(self) -> List[dict]:
        return self._get(f'markets')

    def get_market(self, market: str) -> dict:
        return self._get(f'markets/{market}')

    def get_orderbook(self, market: str, depth: int = None) -> dict:
        return self._get(f'markets/{market}/orderbook', {'depth': depth})

    def get_trades(self, market: str, limit: int = 100, start_time: float = None, end_time: float = None) -> dict:
        return self._get(f'markets/{market}/trades', {'limit':limit, 'start_time': start_time, 'end_time': end_time})

    def get_all_trades(self, market: str, start_time: float = None, end_time: float = None) -> List:
        ids = set()
        limit = 100
        results = []
        while True:
            response = self._get(f'markets/{market}/trades', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            print(f'Adding {len(response)} trades with end time {end_time}')
            if len(response) == 0:
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        return results

    def get_historical_data(self,market_name: str,resolution: int ,limit: int ,start_time: float ,end_time: float ) -> dict:
        return self._get(f'markets/{market_name}/candles', dict(resolution=resolution,limit=limit,start_time=start_time,end_time=end_time))

    def get_future_stats(self, future_name) -> List[dict]:
        return self._get(f'futures/{future_name}/stats', {'future_name' : future_name})

    def get_funding_rates(self,symbol: Optional[str] = None , start_time = None, end_time = None) -> List[dict]:
        if symbol == None:
            return self._get(f'funding_rates')
        else:
            return self._get(f'funding_rates',{'start_time':start_time,'start_time':end_time,'future':symbol})
        
    def get_next_funding_rate(self,symbol):
            return self._get('/futures/{}/stats'.format(symbol))['nextFundingRate']
    
    def get_index_weights(self,index_name) -> dict:
        return self._get(f'indexes/{index_name}/weights')
    
    def getFundingPayments(self, start_time: int = None, end_time: int = None) -> dict:
        return self._get(f'funding_payments')

    def getborrowPayments(self,  start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get(f'spot_margin/borrow_history')
    
    def getlendingPayments(self,  start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get(f'spot_margin/lending_history')
    
    def get_borrow_rates(self,symbol: Optional[str] = None) -> List[dict]:
        if symbol == None:
            return self._get(f'spot_margin/borrow_rates')
        else:
            borrow_rates = self._get(f'spot_margin/borrow_rates')
            for b in borrow_rates:
                if(symbol == b['coin']):
                    return b 
#----------------------------------------------------------------------------------
    def get_total_usd_balance(self) -> int:
        total_usd = 0
        balances = self._get('wallet/balances')
        for balance in balances:
            total_usd += balance['usdValue']
        return total_usd
    
    #Not authorized for subaccount-specific access
    def get_all_balances(self) -> List[dict]:
        return self._get('wallet/all_balances')
    
    #Not authorized for subaccount-specific access
    def get_total_account_usd_balance(self) -> int:
        total_usd = 0
        all_balances = self._get('wallet/all_balances')
        for wallet in all_balances:
            for balance in all_balances[wallet]:
                total_usd += balance['usdValue']
        return total_usd

    def get_all_futures(self) -> List[dict]:
        return self._get('futures')
    
    def get_future(self, future_name: str = None) -> dict:
        return self._get(f'futures/{future_name}')

    def get_all_trades(self, market: str, start_time: float = None, end_time: float = None) -> List:
        ids = set()
        limit = 100
        results = []
        while True:
            response = self._get(f'markets/{market}/trades', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            print(f'Adding {len(response)} trades with end time {end_time}')
            if len(response) == 0:
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        return results
    
    def get_coins(self) -> List[dict]:
        return self._get('wallet/coins')

    def get_expired_futures(self) -> List[dict]:
        return self._get('expired_futures')

    def get_latency_stats(self, days: int = 1, subaccount_nickname: str = None) -> Dict:
        return self._get('stats/latency_stats', {'days': days, 'subaccount_nickname': subaccount_nickname})

    def get_staking_balances(self) -> List[dict]:
        return self._get('staking/balances')

    def get_stakes(self) -> List[dict]:
        return self._get('staking/stakes')

    def get_FR_annual_avg(self,symbol,hours):
        fr_lists = self.get_funding_rates(symbol)
        fr_hours = [fr_lists[i]['rate'] for i in range(hours)]
        avg_hours = sum(fr_hours)*100/hours
        return avg_hours*365*24
    
    def get_funding_payments(self, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get('funding_payments', {
            'start_time': start_time,
            'end_time': end_time
        })
    
    def get_borrow_history(self, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get('spot_margin/borrow_history', {'start_time': start_time, 'end_time': end_time})

    def get_lending_history(self, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get('spot_margin/lending_history', {
            'start_time': start_time,
            'end_time': end_time
        })