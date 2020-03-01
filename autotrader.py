import socket
import select
import matplotlib.pyplot as plt
import numpy as np
import pickle
import pandas as pd
import datetime,time

REMOTE_IP = "35.179.45.135"
UDP_ANY_IP = ""

# USERNAME = "Team04"
# PASSWORD = "9zU2Eh3p"

USERNAME = "Team36"
PASSWORD = "MFYUL4zU"


# -------------------------------------
# EML code (EML is execution market link)
# -------------------------------------

EML_UDP_PORT_LOCAL = 8078
EML_UDP_PORT_REMOTE = 8001

eml_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
eml_sock.bind((UDP_ANY_IP, EML_UDP_PORT_LOCAL))


# -------------------------------------
# IML code (IML is information market link)
# -------------------------------------

IML_UDP_PORT_LOCAL = 7078
IML_UDP_PORT_REMOTE = 7001
IML_INIT_MESSAGE = "TYPE=SUBSCRIPTION_REQUEST"

iml_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
iml_sock.bind((UDP_ANY_IP, IML_UDP_PORT_LOCAL))


# -------------------------------------
# Auto trader
# -------------------------------------

def tryToBuy(feedcode,ask_price,ask_volume,volToBuy,volume):
    volToBuy = min(ask_volume,volToBuy)
    send_order(feedcode, "BUY", ask_price, volToBuy)
    data, addr=eml_sock.recvfrom(1024)
    message = data.decode('utf-8')
    try:
        feedcode,traded_price,traded_volume=handle_message(message)
    except Exception as e:
        error = handle_message(message)
        traded_price=0
        traded_volume=0
        quit()

    volume += traded_volume
    return volume

def tryToSell(feedcode,bid_price,bid_volume,volume):
    # volToSell = min(bid_volume,volume)
    volToSell = bid_volume
    send_order(feedcode, "SELL", bid_price, volToSell)
    data, addr=eml_sock.recvfrom(1024)
    message = data.decode('utf-8')
    try:
        feedcode,traded_price,traded_volume=handle_message(message)
    except Exception as e:
        error = handle_message(message)
        traded_price=0
        traded_volume=0
        quit()

    volume += traded_volume
    return volume

def decideBuy(prices,count):
    smoothed_data = np.convolve(prices,np.ones(3)/3,'valid')
    gradient=np.diff(smoothed_data)
    if len(smoothed_data)>10:
        print("Buy")
        leading_edge = np.mean(gradient[-3:])
        trailing_edge = np.mean(gradient[:-3])
        print(leading_edge,trailing_edge,count,"debug count")
        if (leading_edge>0) and (trailing_edge<=0) and (count>=1):
            return True, count
        elif (leading_edge<0) and (trailing_edge<=0):
            print("here")
            count+=1
            return False, count
        else:
            count-=0
            return False, count
    else:
        return False, 0

def decideSell(prices,count):
    smoothed_data = np.convolve(prices,np.ones(3)/3,'valid')
    gradient=np.diff(smoothed_data)
    if len(prices)>10:
        print("Sell")
        gradient=np.diff(smoothed_data)
        leading_edge = np.mean(gradient[-3:])
        trailing_edge = np.mean(gradient[:-3])
        if (leading_edge<0) and (trailing_edge>=0)  and (count>=1):
            return True,count
        elif (leading_edge>0) and (trailing_edge>=0):
            count+=1
            return False,count
        else:
            count-=0
            return False, count
    else:
        return False, 0

def start_autotrader(status):
    """
        Things to do:
        1. Add thresholds on how much to hold of Sp and esx
        2. Always try to move more volume esx than sp
        3. Esx affects sp as its larger
    """
    current_value = status['current_value']
    volSP = status['volSP']
    volESX = status['volESX']
    print(volSP,volESX)

    maxsize=15
    # subscribe()
    sp_ask_prices=[]
    esx_ask_prices=[]
    sp_bid_prices=[]
    esx_bid_prices=[]

    sp_trade_prices  = []
    esx_trade_prices = []

    sp_buy_threshold = 0
    esx_buy_threshold = 0
    sp_sell_threshold = 1e33
    esx_sell_threshold = 1e33
    counter = 0

    sp_buy_counter=0
    sp_sell_counter=0
    esx_buy_counter=0
    esx_sell_counter=0

    # while True:
    while True:
        # listenInstance()
        data, addr=iml_sock.recvfrom(1024)
        message = data.decode('utf-8')
        comps =  message.split("|")
        type = comps[0]

        if type == "TYPE=TRADE":

            feedcode = comps[1].split("=")[1]
            side = comps[2].split("=")[1]
            traded_price = float(comps[3].split("=")[1])
            traded_volume = int(comps[4].split("=")[1])

            if feedcode == "SP-FUTURE":
                if len(sp_trade_prices)==maxsize:
                    sp_trade_prices = sp_trade_prices[1:maxsize]
                sp_trade_prices.append(traded_price)
            elif feedcode == "ESX-FUTURE":
                if len(esx_trade_prices)==maxsize:
                    esx_trade_prices = esx_trade_prices[1:maxsize]
                esx_trade_prices.append(traded_price)

                # From the data it looks like SP is entrained to ESX. Hence a
                # large buy in SP should push the market up
                # The definition of large buy was based on data - needs to be
                # automated
                if (traded_volume>1000) and (side=="BID"):
                    volSP=tryToBuy("SP-FUTURE",traded_price,
                                   20,20,
                                   volSP)
                if (traded_volume>1000) and (side=="ASK"):
                    volSP=tryToSell("SP-FUTURE",traded_price,
                                    20,20,
                                    volSP)

        if comps[0] == "TYPE=PRICE":
            feedcode   = comps[1].split("=")[1]
            bid_price  = float(comps[2].split("=")[1])
            bid_volume = int(comps[3].split("=")[1])
            ask_price  = float(comps[4].split("=")[1])
            ask_volume = int(comps[5].split("=")[1])

            if feedcode == "SP-FUTURE":
                if len(sp_ask_prices)==maxsize:
                    sp_ask_prices = sp_ask_prices[1:maxsize]
                    sp_bid_prices = sp_bid_prices[1:maxsize]
                sp_ask_prices.append(ask_price)
                sp_bid_prices.append(bid_price)
                decision, sp_buy_counter = decideBuy(sp_ask_prices,sp_buy_counter)
                if decision:
                    print("buy SP")
                    volSP=tryToBuy(feedcode,ask_price,ask_volume,sp_buy_counter*3,
                             volSP)
                    sp_buy_counter = 0

                decision, sp_sell_counter = decideSell(sp_bid_prices,sp_sell_counter)
                if decision:
                    print("sell SP",volSP,bid_volume,sp_sell_counter)
                    volSP=tryToSell(feedcode,bid_price,sp_sell_counter*3,
                             volSP)
                    sp_sell_counter = 0

            elif feedcode == "ESX-FUTURE":
                if len(esx_ask_prices)==maxsize:
                    esx_ask_prices = esx_ask_prices[1:maxsize]
                    esx_bid_prices = esx_bid_prices[1:maxsize]
                esx_ask_prices.append(ask_price)
                esx_bid_prices.append(bid_price)
                decision, esx_buy_counter = decideBuy(esx_ask_prices,esx_buy_counter)
                print(decision, esx_buy_counter)
                if decision:
                    print("buy ESX")
                    volESX=tryToBuy(feedcode,ask_price,ask_volume,esx_buy_counter*3,
                             volESX)
                    esx_buy_counter = 0
                # if bid_price > esx_sell_threshold:
                #     print("buy ESX",volESX,bid_volume)
                #     tryToBuy(feedcode,bid_price,min(volESX,bid_volume),
                #              volESX)

                decision, esx_sell_counter = decideSell(esx_bid_prices,esx_sell_counter)
                if decision:
                    print("sell ESX",volESX,bid_volume,esx_sell_counter)
                    volESX=tryToSell(feedcode,bid_price,esx_sell_counter*3,
                             volESX)
                    esx_sell_counter = 0
            try:
                current_value = (volSP*sp_bid_prices[-1] +
                                 volESX*esx_bid_prices[-1])
            except Exception as e:
                pass

            # print(f"current value: {current_value} current_money: {current_money}")
            current_status={
                'current_value'  : current_value,
                'volSP':volSP,
                'volESX':volESX
            }
            print(current_status)

            # Keep track of current position in case of crash
            print(esx_buy_counter,esx_sell_counter,sp_buy_counter,sp_sell_counter)
            with open('status.pkl', 'wb') as handle:
                pickle.dump(current_status, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # print(message)
        try:
            print(f"Our net worth: {sp_ask_prices[-1]*volSP + esx_bid_prices[-1]*volESX}")
        except Exception as e:
            pass


        # print(sp_ask_prices,esx_ask_prices)

        if counter == 1000:
            counter=0
            fig, (ax1,ax2) = plt.subplots(1,2)
            ax1.set_title('sp')
            ax2.set_title('esx')
            ax1.plot(sp_bid_prices[1:])
            ax1.plot(sp_ask_prices[1:])
            ax2.plot(esx_bid_prices[1:])
            ax2.plot(esx_ask_prices[1:])
            fig.savefig("data.png")
        counter+=1

    return current_status

def actionToTake():
    pass

# def interactivePlot():
#     plt.ion()
#     fig1 = plt.figure()
#     ax1 = fig1.add_subplot(111)
#     fig2 = plt.figure()
#     ax2 = fig2.add_subplot(111)
#     line_esx, = ax1.plot(esx_bid_prices)
#     line_sp, = ax2.plot(sp_bid_prices)
#
#     line_esx.set_ydata(esx_bid_prices)
#     line_sp.set_ydata(sp_bid_prices)
#     for fig in [fig1]:
#         fig.canvas.draw()
#         fig.canvas.flush_events()


def subscribe():
    iml_sock.sendto(IML_INIT_MESSAGE.encode(), (REMOTE_IP, IML_UDP_PORT_REMOTE))

def listenInstance():
    ready_socks,_,_ = select.select([iml_sock, eml_sock], [], [])
    for socket in ready_socks:
        data, addr = socket.recvfrom(1024)
        message = data.decode('utf-8')
        handle_message(message)

def event_listener():
    """
    Wait for messages from the exchange and
    call handle_message on each of them.
    """
    while True:
        ready_socks,_,_ = select.select([iml_sock, eml_sock], [], [])
        for socket in ready_socks:
            data, addr = socket.recvfrom(1024)
            message = data.decode('utf-8')
            handle_message(message)


def handle_message(message):
    comps = message.split("|")

    if len(comps) == 0:
        print(f"Invalid message received: {message}")
        return

    type = comps[0]

    if type == "TYPE=PRICE":

        feedcode = comps[1].split("=")[1]
        bid_price = float(comps[2].split("=")[1])
        bid_volume = int(comps[3].split("=")[1])
        ask_price = float(comps[4].split("=")[1])
        ask_volume = int(comps[5].split("=")[1])

        print(f"[PRICE] product: {feedcode} bid: {bid_volume}@{bid_price} ask: {ask_volume}@{ask_price}")

    if type == "TYPE=TRADE":

        feedcode = comps[1].split("=")[1]
        side = comps[2].split("=")[1]
        traded_price = float(comps[3].split("=")[1])
        traded_volume = int(comps[4].split("=")[1])

        print(f"[TRADE] product: {feedcode} side: {side} price: {traded_price} volume: {traded_volume}")

    if type == "TYPE=ORDER_ACK":

        if comps[1].split("=")[0] == "ERROR":
            error_message = comps[1].split("=")[1]
            print(f"Order was rejected because of error {error_message}.")
            return 0

        feedcode = comps[1].split("=")[1]
        traded_price = float(comps[2].split("=")[1])

        # This is only 0 if price is not there, and volume became 0 instead.
        # Possible cause: someone else got the trade instead of you.
        if traded_price == 0:
            print(f"Unable to get trade on: {feedcode}")
            return

        traded_volume = int(comps[3].split("=")[1])

        print(f"[ORDER_ACK] feedcode: {feedcode}, price: {traded_price}, volume: {traded_volume}")
        return feedcode,traded_price,traded_volume

def time2seconds(t):
    x = time.strptime(t,'%H:%M:%S')
    return int(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds())

# def gradient(times,data):
#     t=np.asarray(times)
#     tt = np.asarray([time2seconds(T) for T in t])
#     print(len(data),len(tt))
#     deltax = (data[1:] - data[:-1])
#     deltat = tt
#     print(len(deltax),len(deltat))
#     gradient = deltax/deltat
#     return gradient

def send_order(target_feedcode, action, target_price, volume):
    """
    Send an order to the exchange.
    :param target_feedcode: The feedcode, either "SP-FUTURE" or "ESX-FUTURE"
    :param action: "BUY" or "SELL"
    :param target_price: Price you want to trade at
    :param volume: Volume you want to trade at. Please start with 10 and go from there. Don't go crazy!
    :return:
    Example:
    If you want to buy  100 SP-FUTURES at a price of 3000:
    - send_order("SP-FUTURE", "BUY", 3000, 100)
    """
    order_message = f"TYPE=ORDER|USERNAME={USERNAME}|PASSWORD={PASSWORD}|FEEDCODE={target_feedcode}|ACTION={action}|PRICE={target_price}|VOLUME={volume}"
    print(f"[SENDING ORDER] {order_message}")
    eml_sock.sendto(order_message.encode(), (REMOTE_IP, EML_UDP_PORT_REMOTE))

def takeOffMean(data):
    return data - data.max()

def plotData():
    prices=pd.read_csv("market_data.csv")
    trades=pd.read_csv("trades.csv")
    sp_prices = prices[prices["Instrument"]=="SP-FUTURE"]
    esx_prices = prices[prices["Instrument"]=="ESX-FUTURE"]
    sp_trades = trades[trades["Traded Instrument"]=="SP-FUTURE"]
    esx_trades = trades[trades["Traded Instrument"]=="ESX-FUTURE"]
    # sp_prices['Timestamp'].apply(time2seconds)
    # esx_prices['Timestamp'].apply(time2seconds)
    # sp_trades['Timestamp'].apply(time2seconds)
    # esx_trades['Timestamp'].apply(time2seconds)
    size = 1000
    # plt.scatter(sp_prices['Timestamp'][-size:],sp_prices['Bid Price'][-size:]-np.min(sp_prices['Bid Price'][-size:]),label='sp_prices',marker='>',facecolor='None',edgecolor='k',s=sp_prices['Traded Volume'][-size:])
    plt.scatter(esx_prices['Timestamp'][-size:],
                takeOffMean(0.5*(esx_prices['Bid Price'][-size:] + esx_prices['Ask Price'][-size:])),
                label='esx_prices_avg')

    plt.scatter(sp_prices['Timestamp'][-size:],
                takeOffMean(sp_prices['Ask Price'][-size:]),
                label='sp_prices_avg')

    # plt.scatter(sp_trades['Timestamp'][-size:],sp_trades['Traded Price'][-size:]-np.min(sp_trades['Traded Price'][-size:]),label='sp_trades',facecolor='b',s=sp_trades['Traded Volume'][-size:]/10)
    plt.scatter(esx_trades['Timestamp'][-size:],
                takeOffMean(esx_trades['Traded Price'][-size:]),
                label='esx_trades',facecolor='None',edgecolor='b',
                s=np.exp(esx_trades['Traded Volume'][-size:])/100)
    plt.scatter(sp_trades['Timestamp'][-size:],
                takeOffMean(sp_trades['Traded Price'][-size:]),
                label='sp_trades',facecolor='None',edgecolor='m',
                s=np.exp(sp_trades['Traded Volume'][-size:])/100)

    plt.legend()
    plt.show()

# -------------------------------------
# Main
# -------------------------------------

if __name__ == "__main__":
    try:
        with open('status.pkl', 'rb') as handle:
            status = pickle.load(handle)
        print("loaded from file")
        print(status)
    except Exception as e:
        print(e)
        status = {
            'current_value'  : 0,
            'volSP':0,
            'volESX':0
            }
    subscribe()
    # start_autotrader(status)
    plotData()
