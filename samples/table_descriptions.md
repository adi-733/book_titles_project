# Table descriptions #
The tables pg_books, pg_books_ca, pg_books_aus contain the metadata and data of the books in the Project Gutenberg (USA, Canada and Australia respectively).

The tables standard_ebooks, fadedpage, books_and_genres are tables imported from different sources in order to match genres for the books from Project Gutenberg (Canada and Australia).

The table genres is used to determine whether or not a certain genre / genre list is fiction or non-fiction.

The table late_appearing_titles is a view, containing books from all three projects, their genres, titles and authors, and the relative location of the first appearing of their title, if the title appears between 1 and 10 times, after half of the text.

The table first_appearance_proof contains the sentence / paragraph in which the title appears for the first time, for the books in late_appearing_titles table.
