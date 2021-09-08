## **About**
- Reconcile AWB number from Tokopedia and Bukalapak for payment fee purpose

## **Clone or Download this Repository**
If you have `git` installed in your machine :
`gh repo clone bintangginanjar/Tool-Rekon`

Clone the repo without git :
- Download the repo : [Download zip](https://github.com/bintangginanjar/Tool-Rekon/archive/refs/heads/master.zip)
- Extract the zip on destination folder

## **Install Python using Anaconda**
- Download Anaconda from following URL: [https://www.anaconda.com/products/individual](https://www.anaconda.com/products/individual)
- Follow the installation steps, and make sure python 3 is successfully installed in your machine by type following command : 

`python --version`

## **Install Python IDE**
You can use your favorite IDE :
- [PyCharm](https://www.jetbrains.com/edu-products/download/#section=pycharm-edu)
- [Visual Code](https://code.visualstudio.com/Download)
- [Spyder](https://docs.spyder-ide.org/current/installation.html)
- [Sublime Text](https://www.sublimetext.com/3)

## **Install Scrapy**
Since we've already install Anaconda, we can install Scrapy using following command

`conda install -c conda-forge scrapy`

Alternatively we can use pip command

`pip install Scrapy`

## **Running Scrapy**
- Enter extracted directory
- Simply put following commands into your shell if you want reconcile Toped's AWB

`scrapy crawl toped`

- Or put following commands into your shell if you want reconcile BL's AWB

`scrapy crawl rekon`