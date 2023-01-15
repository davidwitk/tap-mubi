#!/usr/bin/env python

from tap_mubi.sync import sync_movie_list, sync_movie_data

LIST_ID = "138118"  # Top 1000 Movies by Mubi

def main():
    sync_movie_list(LIST_ID)
    sync_movie_data(LIST_ID)

if __name__ == '__main__':
    main()
