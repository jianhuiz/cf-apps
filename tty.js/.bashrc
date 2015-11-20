#
# ~/.bashrc
#

# If not running interactively, don't do anything
[[ $- != *i* ]] && return

alias ls='ls --color=auto'
PS1='[\u@\h \W]\$ '


alias ll='ls -l'
alias la='ls -a'
alias l.='ls -d .*'

export HISTSIZE=10000
export HISTFILESIZE=10000
export HISTCONTROL=ignoreboth

export PROMPT_COMMAND="history -a"
shopt -s histappend

bind '"\e[A": history-search-backward'
bind '"\e[B": history-search-forward'


