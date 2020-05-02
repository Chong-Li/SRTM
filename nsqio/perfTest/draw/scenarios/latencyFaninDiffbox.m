clear all;

f1 = figure;
hold on;


load log/tcp_fanin_oneway_64_1;
load log/tcp_fanin_oneway_128_1;
load log/tcp_fanin_oneway_256_1;
load log/tcp_fanin_oneway_512_1;

load log/udp_fanin_oneway_64_1;
load log/udp_fanin_oneway_128_1;
load log/udp_fanin_oneway_256_1;
load log/udp_fanin_oneway_512_1;

load log/mtc_fanin_oneway_64_1;
load log/mtc_fanin_oneway_128_1;
load log/mtc_fanin_oneway_256_1;
load log/mtc_fanin_oneway_512_1;

M = [tcp_fanin_oneway_64_1(1:9000,:)/1000000 udp_fanin_oneway_64_1(1:9000,:)/1000000  ...
    tcp_fanin_oneway_128_1(1:9000,:)/1000000 udp_fanin_oneway_128_1(1:9000,:)/1000000  ...
    tcp_fanin_oneway_256_1(1:9000,:)/1000000 udp_fanin_oneway_256_1(1:9000,:)/1000000 ...
    tcp_fanin_oneway_512_1(1:9000,:)/1000000 udp_fanin_oneway_512_1(1:9000,:)/1000000 ;

mean(udp_fanin_oneway_256_1)

h1 = boxplot(M, 'colors', 'kbkbkbkb', 'notch', 'on', 'Labels', ...
{'TCP 64','UDP 64',  'TCP 128','UDP 128',  'TCP 256','UDP 256', 'TCP 512','UDP 512'});

tx = [2.5 2.5];
ty = [-10 900000];
h = line(tx, ty, 'color', 'k', 'linewidth', 2);

tx = [4.5 4.5];
h = line(tx, ty, 'color', 'k', 'linewidth', 2);

tx = [6.5 6.5];
h = line(tx, ty, 'color', 'k', 'linewidth', 2);

xlabel('Protocol and Number of Concurrent Connections');
ylabel('Latency (ms)', 'fontsize', 15);
%xlabel('Number of Subscribers', 'fontsize', 15);
%title('Time spent in loop from step3 to step7, 2k-payload packets sent fro m Domain2 and Domain3 ', 'fontsize', 20);

grid on;
ylim([0 0.5])
