#!/usr/sbin/dtrace -s

#pragma D option quiet

dtrace:::BEGIN
{
    
}

io:::start
{
    start_time[arg0] = timestamp;
}

io:::done
/this->start = start_time[arg0]/
{
    this->delta = timestamp - this->start;
    @a[args[1]->dev_major, args[1]->dev_minor] = avg(this->delta);
    start_time[arg0] = 0;
}

tick-1s
{
    printa("%d\t%d\t%@d\n", @a);
    trunc(@a);
}

dtrace:::END
{
    
}
