#! /bin/sh
asv run  -E existing -b '^increment.Merge(.*)'         
asv run  -E existing -b 'original_increment.Merge(.*)' 
asv run  -E existing -b 'original_merge.Merge(.*)'     
asv run  -E existing -b '^merge_increment.Merge(.*)'   
