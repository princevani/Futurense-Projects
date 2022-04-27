# Required Libraries and Packages
import time
from tkinter import filedialog
import pyautogui as pyg
import os
import datetime

# Declaring variables
n = 4
stars = 1

# Dialog Box for the folder path
folder = filedialog.askdirectory()

# Triangle Pattern
for i in range(n):

    # Current time
    current = datetime.datetime.now().replace(microsecond=0)
    fmt = " %y_%b_%d_%H_%M_%S"
    new_time = datetime.datetime.strftime(current, fmt)

    for j in range(0, n - 1):
        print(" ", end="")
    print("* " * stars)

    # Delay for 2 Seconds
    time.sleep(2)

    # Taking SS of the pattern during the interation
    screen = pyg.screenshot()

    # Updating the path and name of the screenshots and saving it to the folder
    file = os.path.join(folder, f"hello{new_time}.png")
    screen.save(file)

    stars += 1
    n -= 1
