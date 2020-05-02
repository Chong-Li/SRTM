clear all;

f1 = figure;
hold on;

tcp_64 =[];
tcp_128 = [];
tcp_256 = [];
tcp_512 = [];
udp_64 = [];
udp_128 = [];
udp_256 = [];
udp_512 = [];
mtc_64 = [];
mtc_128 = [];
mtc_256 = [];
mtc_512 = [];

% for k = 1
%     tcp_64 = [tcp_64; load(strcat('log_last/tcp_fanout_oneway_64_',num2str(k*2)))];
%     tcp_128 = [tcp_128; load(strcat('log_last/tcp_fanout_oneway_128_',num2str(k*4)))];
%     tcp_256 = [tcp_256; load(strcat('log_last/tcp_fanout_oneway_256_',num2str(k*8)))];
%     tcp_512 = [tcp_512; load(strcat('log_last/tcp_fanout_oneway_512_',num2str(k*16)))];
%     udp_64 = [udp_64; load(strcat('log_last/udp_fanout_oneway_64_',num2str(k*2)))];
%     udp_128 = [udp_128; load(strcat('log_last/udp_fanout_oneway_128_',num2str(k*4)))];
%     udp_256 = [udp_256; load(strcat('log_last/udp_fanout_oneway_256_',num2str(k*8)))];
%     udp_512 = [udp_512; load(strcat('log_last/udp_fanout_oneway_512_',num2str(k*16)))];
%     mtc_64 = [mtc_64; load(strcat('log_last/mtc_fanout_oneway_64_',num2str(k*2)))];
%     mtc_128 = [mtc_128; load(strcat('log_last/mtc_fanout_oneway_128_',num2str(k*4)))];
%     mtc_256 = [mtc_256; load(strcat('log_last/mtc_fanout_oneway_256_',num2str(k*8)))];
%     mtc_512 = [mtc_512; load(strcat('log_last/mtc_fanout_oneway_512_',num2str(k*16)))];
% end


tcp_64 = [tcp_64; load('log_last/tcp_fanout_oneway_64_1')];
tcp_128 = [tcp_128; load('log_last/tcp_fanout_oneway_128_1')];
tcp_256 = [tcp_256; load('log_last/tcp_fanout_oneway_256_1')];
tcp_512 = [tcp_512; load('log_last/tcp_fanout_oneway_512_1')];
udp_64 = [udp_64; load('log_last/udp_fanout_oneway_64_1')];
udp_128 = [udp_128; load('log_last/udp_fanout_oneway_128_1')];
udp_256 = [udp_256; load('log_last/udp_fanout_oneway_256_1')];
udp_512 = [udp_512; load('log_last/udp_fanout_oneway_512_1')];
mtc_64 = [mtc_64; load('log_last/mtc_fanout_oneway_64_1')];
mtc_128 = [mtc_128; load('log_last/mtc_fanout_oneway_128_1')];
mtc_256 = [mtc_256; load('log_last/mtc_fanout_oneway_256_1')];
mtc_512 = [mtc_512; load('log_last/mtc_fanout_oneway_512_1')];

% tcp = load('tcp_fanout_oneway_1_1');
% udp = load('udp_fanout_oneway_1_1');
% mtc = load('mtc_fanout_oneway_1_1');
%
% M = [tcp udp mtc];
%
% h1 = boxplot(M, 'colors', 'kbr', 'notch', 'on', 'labels',...
%     {'TCP 64','UDP 64', 'MTC 64'});

%M = [tcp_64 udp_64 tcp_128 udp_128 tcp_256 udp_256 tcp_512 udp_512]/1000000;

%h1 = boxplot(M, 'colors', 'kbkbkbkb', 'notch', 'on', 'labels',...
%    {'TCP 64','UDP 64', 'TCP 128','UDP 128', 'TCP 256','UDP 256', 'TCP 512','UDP 512'});
% tx = [2.5 2.5];
% ty = [-10 200000];
% h = line(tx, ty, 'color', 'k', 'linewidth', 2);
%
% tx = [4.5 4.5];
% h = line(tx, ty, 'color', 'k', 'linewidth', 2);
%
% tx = [6.5 6.5];
% h = line(tx, ty, 'color', 'k', 'linewidth', 2);

M = [tcp_64 udp_64 mtc_64 tcp_128 udp_128 mtc_128 tcp_256 udp_256 mtc_256 tcp_512 udp_512 mtc_512]/1000000;

h1 = boxplot(M, 'colors', 'kbrkbrkbrkbrkbr', 'notch', 'on', 'labels',...
    {'TCP 64','UDP 64', 'MTC 64', 'TCP 128','UDP 128', 'MTC 128', 'TCP 256','UDP 256','MTC 256', 'TCP 512','UDP 512', 'MTC 512'});

tx = [3.5 3.5];
ty = [-10 200000];
h = line(tx, ty, 'color', 'k', 'linewidth', 2);

tx = [6.5 6.5];
h = line(tx, ty, 'color', 'k', 'linewidth', 2);

tx = [9.5 9.5];
h = line(tx, ty, 'color', 'k', 'linewidth', 2);

% M = [mtc_64 mtc_128 mtc_256 mtc_512]/1000000;
% h1 = boxplot(M, 'colors', 'kkkk', 'notch', 'on', 'labels',...
%     {'MTC 64', 'MTC 128', 'MTC 256', 'MTC 512'});
%

% tx = [2.5 2.5];
% ty = [-10 200000];
% h = line(tx, ty, 'color', 'k', 'linewidth', 2);
%
% tx = [4.5 4.5];
% h = line(tx, ty, 'color', 'k', 'linewidth', 2);
%
% tx = [6.5 6.5];
% h = line(tx, ty, 'color', 'k', 'linewidth', 2);

xlabel('Protocol and Number of Concurrent Connections');
ylabel('Latency (ms)', 'fontsize', 15);
%xlabel('Number of Subscribers', 'fontsize', 15);
%title('Time spent in loop from step3 to step7, 2k-payload packets sent fro m Domain2 and Domain3 ', 'fontsize', 20);

grid on;
ylim([0 10]);

mean(tcp_64)

mean(udp_64)



mean(mtc_64)
mean(tcp_128)
mean(udp_128)

mean(mtc_128)
mean(tcp_256)
mean(udp_256)
mean(mtc_256)
mean(tcp_512)
mean(udp_512)
mean(mtc_512)
