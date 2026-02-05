#!/usr/bin/env python3
"""Add test strategy for admin user."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
import json

engine = create_engine('mysql+pymysql://root:password@127.0.0.1:3306/tradermate?charset=utf8mb4')

# Simple strategy code
code = '''
from vnpy.app.cta_strategy import CtaTemplate, BarGenerator
from vnpy.trader.object import BarData, TickData

class SimpleMAStrategy(CtaTemplate):
    """Simple Moving Average Strategy."""
    
    ma_window = 20
    fixed_size = 1
    
    parameters = ["ma_window", "fixed_size"]
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.bg = BarGenerator(self.on_bar)
        self.ma_value = 0
        self.prices = []
    
    def on_bar(self, bar: BarData):
        self.prices.append(bar.close_price)
        if len(self.prices) > self.ma_window:
            self.prices.pop(0)
        
        if len(self.prices) >= self.ma_window:
            self.ma_value = sum(self.prices) / len(self.prices)
            
            if bar.close_price > self.ma_value and self.pos == 0:
                self.buy(bar.close_price, self.fixed_size)
            elif bar.close_price < self.ma_value and self.pos > 0:
                self.sell(bar.close_price, self.pos)
'''

with engine.connect() as conn:
    # Check if already exists
    result = conn.execute(text('SELECT id FROM strategies WHERE user_id = 1 AND name = :name'), 
                          {'name': 'Simple MA Strategy'})
    if result.fetchone():
        print('Strategy already exists for admin user')
    else:
        # Insert a strategy for admin (user_id=1)
        conn.execute(text('''
            INSERT INTO strategies (user_id, name, class_name, description, parameters, code, is_active, created_at, updated_at)
            VALUES (1, 'Simple MA Strategy', 'SimpleMAStrategy', 'Simple moving average crossover strategy', 
                    :params, :code, 1, NOW(), NOW())
        '''), {'params': json.dumps({"ma_window": 20, "fixed_size": 1}), 'code': code})
        conn.commit()
        print('Strategy inserted for admin user')
    
    # Verify
    result = conn.execute(text('SELECT id, user_id, name FROM strategies WHERE user_id = 1'))
    for row in result.fetchall():
        print(f'  - {row}')
