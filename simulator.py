import gradio as gr
import pandas as pd
import numpy as np
model_list = ["dow30_high",
              "DOW30_DDPG_high_C3_little",
              "DOW30_TD3_high_C3_little",
              "DOW30_DDPG_high_C5_little",
              "DOW30_TD3_high_C5_little",
              "DOW30_TD3_high_C3",
              "DOW30_DDPG_high_C3",
              "DOW30_DDPG_high_B3",
              "DOW30_TD3_high_B3",
              "aapl_ddpg_low", 
              "aapl_td3_low",
              "aapl_ddpg_high",
              "aapl_td3_high"]

asset_list = ["Dow30","AAPL"]

def currency_mask(text):
    return "{:0>12,.2f}".format(float(text))

def clean_number(text):
    return text.replace(",", "")

def save_profile(name, user_id):
    return f"Profile of  {name} user id {user_id} saved succesfully"

def add_money(amount):
    new_balance = 100 + float(amount)   
    return f"New balance: ${new_balance}"

def update_image():
    return "https://via.placeholder.com/150"  

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
 
def plot_pie(labels, sizes):
     
    sns.set(style="whitegrid")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', wedgeprops=dict(width=0.3))
    
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)
    
    plt.axis('equal')  
    plt.title('Portifolio Distribution')  
 
    plt.savefig("images/portifolio_pie_plot.png")
    plt.close()

    
def plot_linechart():
    data = pd.read_csv("results/asset_memory.csv")    
    plt.figure(figsize=(10, 6))
    
 
    plt.plot(data['date'], data['account_value'], marker='o')
    
    plt.xlabel('Date')  
    plt.ylabel('Account Value')  
    
      
    tick_spacing = 5 
    tick_labels = data['date'][::tick_spacing]  
    tick_positions = np.arange(len(data['date']))[::tick_spacing]  
    

    plt.xticks(ticks=tick_positions, labels=tick_labels, rotation=45)
    
    
    plt.title('Account Value through time')   
    plt.legend()   
    plt.xticks(rotation=45)   
    plt.grid(True)  
    plt.savefig("images/assets_line_plot.png")
    plt.show()
    plt.close()
def run_simulation(tickers, model, money, start_date, finish_date):
    from brainLib.brainTrader import GenericTrader
    import matplotlib.pyplot as plt
    import pandas as pd
    print("money",money)
    print("start_date",start_date)
    print("end_date",finish_date)

    trader = GenericTrader()
    symbol = tickers
    
    # Exception for multipaper
    if tickers=="Dow30":
        tickers = ['AAPL', 'AMGN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS','GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD', 'MMM',
       'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH', 'V', 'VZ', 'WBA', 'WMT']
        symbol =""
    
        
    trading_data_set="data/synthetic_2025_v1.csv" 
    simulation_args =  {"agent_path"    : "agents/"+model+".mdl",
                        "agent_type"    : "ddpg",
                        "data_path"     : trading_data_set,
                        "trade_limit"   : 100,
                        "initial_amount": str(money),
                        "start_date"    : start_date,
                        "env"           :  "",
                        "end_date"      : finish_date,
                        "symbol"        : symbol,
                        "user"          : "Generic Simulation",
                        "resume_session": False
                       }

    account, actions,env = trader.start_simulation(**simulation_args)   
 
    
    _ , _, num_stock_shares = trader.get_last_session("Generic Simulation")

    print(num_stock_shares)
    print(tickers)
    plot_linechart()
    try:
    
        if len(tickers) > 1:
            tickers, num_stock_shares = zip(*[(ticker, share) for ticker, share in zip(tickers, num_stock_shares) if share > 0])
            plot_pie(tickers,num_stock_shares)
       
    except Exception as e:
        print("error:", e)
        pass
    
    results = pd.read_csv("results/state_memory.csv")
    data    = pd.read_csv("results/asset_memory.csv")
    return currency_mask(float(round(data.tail(1)['account_value'],5))) , "images/portifolio_pie_plot.png", "images/assets_line_plot.png", results, "## BR@IN <br>  > Do not overthink it, use our brain"

with gr.Blocks() as app:
    gr.Markdown("**Final Asset Value:**")
    with gr.Tabs():
         with gr.TabItem("Batch Trading Simulation"):
            with gr.Column():
                with gr.Row():
                    with gr.Column():
                        ticker_dropdown = gr.Dropdown(label="Ticker",
                                                      choices=asset_list,
                                                      info="Symbols to trade")
                        model_dropdown = gr.Dropdown(label="Model", 
                                                     choices=model_list,
                                                     info="Choose the type of algorithm (BRAIN) you wanto to use")
                        money_input = gr.Number(label="Money", 
                                                step=0.01,
                                               info="Initial Balance")
                        upper_buy    = gr.Textbox(label = "Upper Buy Price")
                        upper_sell   = gr.Textbox(label = "Minimum Sell Price")
                        upper_shares = gr.Textbox(label = "Maximum Shares per Day")
                         
                        start_date_input = gr.Textbox(label="Start (YYYY-MM-DD)")
                        finish_date_input = gr.Textbox(label="Finish (YYYY-MM-DD)")
                        run_button = gr.Button("Run")
                    with gr.Column():
                        with gr.Row():
                            final_asset_label = gr.Label("", label="Asset Value")
                        with gr.Row():
                          
                            final_asset_image = gr.Image(label="Portifolio distribution")
                        with gr.Row():
                            comparison_image = gr.Image(label="Asset Value through time")

                with gr.Row():
                    csv_display = gr.Dataframe()

                markdown_results = gr.Markdown()

                run_button.click(
                    fn=run_simulation,
                    inputs=[ticker_dropdown, model_dropdown, money_input, start_date_input, finish_date_input],
                    outputs=[final_asset_label, final_asset_image, comparison_image, csv_display, markdown_results]
                )
                
app.launch(debug=True)
 