"""
COMP.CS.100 Ohjelmointi 1 / Programming 1

Project authors:
Name: Lauri Koivuniemi
Name: Samu Saronsalo


Hangman graphical interface
"""
from tkinter import *

ALPHABET = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
            "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"]


class Hangman_Interface:

    def __init__(self):

        self.__mainwindow = Tk()
        self.__mainwindow.title("Hangman game")
        self.__guesses = 0

        self.__mainwindow.geometry("600x500")
        self.__mainwindow.rowconfigure(0, weight=1)
        self.__mainwindow.columnconfigure(0, weight=1)

        self.__button_list = []

        self.__letter_buttons = {}

        for i in ALPHABET:
            self.__letter_buttons[i] = Button(self.__mainwindow, text=i,
                                              background="white",
                                              command=lambda
                                                  i=i: self.check_guessed_letter(
                                                  i),
                                              state=DISABLED)

            self.__button_list.append(self.__letter_buttons[i])

            row_no = 6

            for button in self.__button_list:
                self.__mainwindow.rowconfigure(row_no, weight=1)
                row_no += 1

            col_no = 0

            for a in self.__button_list:
                self.__mainwindow.columnconfigure(col_no, weight=1)
                col_no += 1
            n = 0
            for j in self.__button_list:
                if 0 <= n <= 8:
                    j.grid(row=6, column=n, sticky="nsew")

                elif 9 <= n <= 17:
                    j.grid(row=7, column=n - 9, sticky="nsew")

                else:
                    j.grid(row=8, column=n - 18, sticky="nsew")

                n += 1
            """
            if 0 <= n <= 8:
                self.__letter_buttons[i].grid(row=6, column=n, sticky=E+W)

            elif 9 <= n <= 17:
                self.__letter_buttons[i].grid(row=7, column=n - 9, sticky=E + W)

            else:
                self.__letter_buttons[i].grid(row=8, column=n - 18, sticky=E + W)

            n += 1

            """

        self.__bg_photo = PhotoImage(file="backgroundphoto.png")
        self.__photos = [PhotoImage(file="hangmanphoto0.png"),
                         PhotoImage(file="hangmanphoto1.png"),
                         PhotoImage(file="hangmanphoto2.png"),
                         PhotoImage(file="hangmanphoto3.png"),
                         PhotoImage(file="hangmanphoto4.png"),
                         PhotoImage(file="hangmanphoto5.png"),
                         PhotoImage(file="hangmanphoto6.png"),
                         PhotoImage(file="hangmanphoto7.png")]

        self.__start_photo = Label(self.__mainwindow,
                                        image=self.__photos[0])

        self.__start_photo.grid(row=0, column=0, columnspan=9, rowspan=5)

        self.__write_word_button = Button(self.__mainwindow,
                                          text="Press this to write your word",
                                          relief=GROOVE, background="white",
                                          command=self.open_new_window)

        self.__error_label = Label(self.__mainwindow, text="")

        self.__write_word_button.grid(row=5, column=0, columnspan=9,
                                      sticky=E + W)

        self.__mainwindow.mainloop()

    def open_new_window(self):

        self.__new_window = Toplevel(self.__mainwindow)

        self.__word = Entry(self.__new_window)

        self.__enter_word_button = Button(self.__new_window, text="Enter",
                                          background="blue",
                                          command=self.initialize_word)

        self.__word.grid(row=0, column=0, columnspan=6, sticky=E + W)

        self.__enter_word_button.grid(row=0, column=7, columnspan=3,
                                      sticky=E + W)

        self.__error_label = Label(self.__new_window, text="")

    def initialize_word(self):

        word = self.__word.get()

        self.__word_letters = []
        self.__word_underscores = []
        wrong_chars = []

        for letter in word:
            letter = letter.upper()

            if letter not in ALPHABET:
                wrong_chars.append(letter)
                self.__word.delete(0, "end")
                self.__error_label.config(text="Only letters allowed")
                self.__error_label.grid(row=1, column=0, columnspan=6)

            else:
                self.__word_letters.append(letter)
                self.__word_underscores.append(" _ ")

        if len(wrong_chars) == 0:
            self.__new_window.destroy()
            self.__write_word_button.destroy()
            self.configure_main_window()

            self.enable_keyboard()

    def configure_main_window(self):

        self.__displayed_word = ""

        for x in self.__word_underscores:
            self.__displayed_word += x

        self.__guessable_word = Label(self.__mainwindow,
                                      text=self.__displayed_word)
        self.__guessable_word.grid(row=5, column=0, columnspan=9, sticky=E + W)

    def check_guessed_letter(self, i):

        if i in self.__word_letters:
            self.__letter_buttons[i].configure(state=DISABLED,
                                               disabledforeground="Green")

            index = 0
            for index in range(len(self.__word_letters)):

                if i == self.__word_letters[index]:
                    self.__word_underscores[index] = i

                if self.__word_underscores.count(" _ ") == 0:
                    self.disable_keyboard()

            self.configure_main_window()


        else:

            self.__letter_buttons[i].configure(state=DISABLED,
                                               disabledforeground="Red")

            self.__guesses += 1
            self.insert_photo()
            if self.__guesses == 7:
                self.disable_keyboard()

    def disable_keyboard(self):
        for x in self.__letter_buttons:
            self.__letter_buttons[x].config(state=DISABLED)

    def enable_keyboard(self):
        for x in self.__letter_buttons:
            self.__letter_buttons[x].config(state=NORMAL)

    def insert_photo(self):

        self.__hangman_photo = Label(self.__mainwindow,
                                     image=self.__photos[self.__guesses])

        self.__hangman_photo.grid(row=0, column=0, columnspan=9, rowspan=5)

def main():
    IF = Hangman_Interface()


if __name__ == "__main__":
    main()

