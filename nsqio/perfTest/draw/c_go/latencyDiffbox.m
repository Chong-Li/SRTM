clear all;

f1 = figure;
hold on;

load go_udp_latency.log;
load go_tcp_latency.log;
%load udp_latency.log;
%load tcp_latency.log;
%load mtc_latency.log;

M = [go_tcp_latency(1:9000,:) go_udp_latency(1:9000,:) ]/1000000;


h1 = boxplot(M, 'colors', 'kbr', 'notch', 'on', 'Labels', {'Go TCP', 'GO UDP'});


xlabel('Implementation Language and Protocol', 'fontsize', 20)
ylabel('Latency (ms)', 'fontsize', 20);

grid on;
ylim([0 0.8]);
