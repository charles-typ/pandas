#! /bin/sh
asv profile  -E existing '^increment.Merge(.*)'         &> report/increment
asv profile  -E existing 'original_increment.Merge(.*)' &> report/original_increment
asv profile  -E existing 'original_merge.Merge(.*)'     &> report/original_merge
asv profile  -E existing '^merge_increment.Merge(.*)'             &> report/merge_increment
