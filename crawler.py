# coding=utf-8

from bs4 import BeautifulSoup
import urllib

def getHTML(url):
  f = urllib.urlopen(url)
  return f.read()

cutAfter = (
  'BetCoin',
  'Bit Elfin',
  'Bit-Advantage.com',
  'Bitcoin King',
  'Bitcoin-Play.com',
  'Bitcoin-Roulette.com',
  'Bitcoin-Stocks',
  'BitcoinChaser',
  'bitcoindice.io',
  'BitcoinSpout',
  'BitMillions.com',
  'Bitoomba Roulette',
  'Bitstamp Hack Mixer',
  'Biz27B-6',
  'Block Chain Lottery',
  'BlockReward.IO',
  'BTC Dice',
  'BTC Gambling',
  'BTC Multiplier',
  'BtcLucky',
  'BTCOracle',
  'Chainroll',
  'Coingig.com',
  'DICEonCRACK',
  'ePay.info',
  'Farset Labs',
  'FrozenBit',
  'girlbtc',
  'HHTT Mining',
  'HoB',
  'JackpotDICE',
  'Just-Dice.com',
  'luckybit',
  'Marjolein Kramer',
  'SatoshiBONES',
  'SatoshiDICE',
  'Satoshijackpot',
  'SatoshiRoulette',
  'Secondstrade',
  'SerpCoin',
  'TAABL',
  'TossBit.com',
  'World Cup',
  'yabtcl',
  )


baseURL = 'https://blockchain.info/tags?offset='
offsetIncrement = 200
offset = 0
data = []

while True:
  print "Offset: " + str(offset)
  html = getHTML(baseURL + str(offset))
  soup = BeautifulSoup(html, 'lxml')
  table = soup.find_all('table')[1]
  tableBody = table.find('tbody')

  rows = tableBody.find_all('tr')
  if not rows:
    break;

  for row in rows:
    cols = row.find_all('td')
    cols = [ele.text.strip() for ele in cols]
    data.append([ele for ele in cols if ele])

  offset += offsetIncrement

print data

try:
  with open("identities.txt", "w") as outfile:
    for row in data:
      for cut in cutAfter:
       if row[1].startswith(cut):
          row[1] = (row[1])[:len(cut)]

      line = row[0] + ", " + row[1]
      outfile.write(line.encode('utf-8').strip() + "\n")
except Exception:
    pass
