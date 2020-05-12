#!/bin/bash
##############################################################################
# ESGF wget download script
#
# Template version: 0.1  (Based on esg-search's wget script template version 1.2)
# Generated on {{timestamp}}
# Datasets:
{% for dataset in datasets %}#   {{dataset}}
{% endfor %}#
###############################################################################
# first be sure it's bash... anything out of bash or sh will break
# and the test will assure we are not using sh instead of bash
if [ $BASH ] && [ `basename $BASH` != bash ]; then
    echo "######## This is a bash script! ##############" 
    echo "Change the execution bit 'chmod u+x $0' or start with 'bash $0' instead of sh."
    echo "Trying to recover automatically..."
    sleep 1
    /bin/bash $0 $@
    exit $?
fi

version=0.1
CACHE_FILE=.$(basename $0).status

#These are the embedded files to be downloaded
download_files="$(cat <<EOF--dataset.file.url.chksum_type.chksum
{% for file in files %}'{{file.filename}}' '{{file.url}}' '{{file.checksum_type}}' '{{file.checksum}}'
{% endfor %}EOF--dataset.file.url.chksum_type.chksum
)"

check_os() {
    local os_name=$(uname | awk '{print $1}')
    case ${os_name} in
        Linux)
            ((debug)) && echo "Linux operating system detected"
            LINUX=1
            MACOSX=0
            ;;
        Darwin)
            ((debug)) && echo "Mac OS X operating system detected"
            LINUX=0
            MACOSX=1
            ;;
        *)
            echo "Unrecognized OS [${os_name}]"
            return 1
            ;;
    esac
    return 0
}

#taken from http://stackoverflow.com/a/4025065/1182464
vercomp () {
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i={{'${#ver1[@]}'}}; i<{{'${#ver2[@]}'}}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<{{'${#ver1[@]}'}}; i++))
    do
        if [[ -z {{'${ver2[i]}'}} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if (({{'10#${ver1[i]}'}} > {{'10#${ver2[i]}'}}))
        then
            return 1
        fi
        if (({{'10#${ver1[i]}'}} < {{'10#${ver2[i]}'}}))
        then
            return 2
        fi
    done
    return 0
}

check_commands() {
    #check wget
    local MIN_WGET_VERSION=1.10
    vercomp $(wget -V | sed -n 's/^.* \([1-9]\.[0-9.]*\) .*$/\1/p') $MIN_WGET_VERSION
    case $? in
        2) #lower
            wget -V
            echo
            echo "** ERROR: wget version is too old. Use version $MIN_WGET_VERSION or greater. **" >&2
            exit 1
    esac
}

usage() {
    echo "Usage: $(basename $0) [flags]"
    echo "Flags is one of:"
    sed -n '/^while getopts/,/^done/  s/^\([^)]*\)[^#]*#\(.*$\)/\1 \2/p' $0
    echo
    echo "This command stores the states of the downloads in .$0.status"
}

#defaults
debug=0
clean_work=1

#parse flags
while getopts ':c:pfF:o:w:isuUndvqhHI:T' OPT; do
    case $OPT in
        F) input_file="$OPTARG";;       #<file> : read input from file instead of the embedded one (use - to read from stdin)
        w) output="$OPTARG";;           #<file> : Write embedded files into a file and exit
        U) update_files=1;;             #       : Update files from server overwriting local ones (detect with -u)
        n) dry_run=1;;                  #       : Don't download any files, just report.
        p) clean_work=0;;               #       : preserve data that failed checksum
        d) verbose=1;debug=1;;          #       : display debug information
        v) verbose=1;;                  #       : be more verbose
        q) quiet=1;;                    #       : be less verbose
        h) usage && exit 0;;            #       : displays this help
        \?) echo "Unknown option '$OPTARG'" >&2 && usage && exit 1;;
        \:) echo "Missing parameter for flag '$OPTARG'" >&2 && usage && exit 1;;
    esac
done
shift $(($OPTIND - 1))

#setup input as desired by the user
if [[ "$input_file" ]]; then
    if [[ "$input_file" == '-' ]]; then
        download_files="$(cat)" #read from STDIN
        exec 0</dev/tty #reopen STDIN as cat closed it
    else
        download_files="$(cat $input_file)" #read from file
    fi
fi

#if -w (output) was selected write file and finish:
if [[ "$output" ]]; then
    #check the file
    if [[ -f "$output" ]]; then
        read -p "Overwrite existing file $output? (y/N) " answ
        case $answ in y|Y|yes|Yes);; *) echo "Aborting then..."; exit 0;; esac
    fi
    echo "$download_files">$output
    exit
fi

#assure we have everything we need
check_commands

check_chksum() {
    local file="$1"
    local chk_type=$2
    local chk_value=$3
    local local_chksum=Unknown

    case $chk_type in
        md5) local_chksum=$(md5sum_ $file | cut -f1 -d" ");;
        sha256) local_chksum=$(sha256sum_ $file|awk '{print $1}'|cut -d ' ' -f1);;
        *) echo "Can't verify checksum." && return 0;;
    esac

    #verify
    ((debug)) && echo "local:$local_chksum vs remote:$chk_value" >&2
    echo $local_chksum
}

#Our own md5sum function call that takes into account machines that don't have md5sum but do have md5 (i.e. mac os x)
md5sum_() {
    hash -r
    if type md5sum >& /dev/null; then
        echo $(md5sum $@)
    else
        echo $(md5 $@ | sed -n 's/MD5[ ]*\(.*\)[^=]*=[ ]*\(.*$\)/\2 \1/p')
    fi
}

#Our own sha256sum function call that takes into account machines that don't have sha256sum but do have sha2 (i.e. mac os x)
sha256sum_() {
    hash -r
    if type sha256sum >& /dev/null; then
        echo $(sha256sum $@)
    elif type shasum >& /dev/null; then
        echo $(shasum -a 256 $@)
    else
        echo $(sha2 -q -256 $@)
    fi
}

get_mod_time_() {
    if ((MACOSX)); then
        #on a mac modtime is stat -f %m <file>
        echo "$(stat -f %m $@)"
    else
        #on linux (cygwin) modtime is stat -c %Y <file>
        echo "$(stat -c %Y $@)"
    fi
    return 0;
}

remove_from_cache() {
    local entry="$1"
    local tmp_file="$(grep -ve "^$entry" "$CACHE_FILE")"
    echo "$tmp_file" > "$CACHE_FILE"
    unset cached
}

download() {
    wget="wget ${quiet:+-q} ${quiet:--v}"
    
    while read line
    do
        # read csv here document into proper variables
        eval $(awk -F "' '" '{$0=substr($0,2,length($0)-2); $3=tolower($3); print "file=\""$1"\";url=\""$2"\";chksum_type=\""$3"\";chksum=\""$4"\""}' <(echo $line) )

        #Process the file
        echo -n "$file ..."

        #get the cached entry if any.
        cached="$(grep -e "^$file" "$CACHE_FILE")"
        
        #if we have the cache entry but no file, clean it.
        if [[ ! -f $file && "$cached" ]]; then
            #the file was removed, clean the cache
            remove_from_cache "$file"
            unset cached
        fi
        
        #check it wasn't modified
        if [[ -n "$cached" && "$(get_mod_time_ $file)" == $(echo "$cached" | cut -d ' ' -f2) ]]; then
                    if [[ "$chksum" == "$(echo "$cached" | cut -d ' ' -f3)" ]]; then
                echo "Already downloaded and verified"
                continue
            elif ((update_files)); then
                #user want's to overwrite newer files
                rm $file
                remove_from_cache "$file"
                unset cached
            else
                #file on server is different from what we have. 
                echo "WARNING: The remote file was changed (probably a new version is available). Use -U to Update/overwrite"
                continue
            fi
        fi
        unset chksum_err_value chksum_err_count

        while : ; do
            # (if we had the file size, we could check before trying to complete)
            echo "Downloading"
            [[ ! -d "$(dirname "$file")" ]] && mkdir -p "$(dirname "$file")"
            if ((dry_run)); then
                #all important info was already displayed, if in dry_run mode just abort
                #No status will be stored
                break
            else
                $wget -O "$file" $url || { failed=1; break; }  
            fi

            #check if file is there
            if [[ -f $file ]]; then
                ((debug)) && echo file found
                if [[ ! "$chksum" ]]; then
                    echo "Checksum not provided, can't verify file integrity"
                    break
                fi
                result_chksum=$(check_chksum "$file" $chksum_type $chksum)
                if [[ "$result_chksum" != "$chksum" ]]; then
                    echo "  $chksum_type failed!"
                    if ((clean_work)); then
                        if !((chksum_err_count)); then
                                chksum_err_value=$result_chksum
                                chksum_err_count=2
                            elif ((checksum_err_count--)); then
                                if [[ "$result_chksum" != "$chksum_err_value" ]]; then
                                    #this is a real transmission problem
                                    chksum_err_value=$result_chksum
                                    chksum_err_count=2
                                fi
                            else
                                #ok if here we keep getting the same "different" checksum
                                echo "The file returns always a different checksum!"
                                echo "Contact the data owner to verify what is happening."
                                echo
                                sleep 1
                                break
                            fi
                        
                            rm $file
                            #try again
                            echo -n "  re-trying..."
                            continue
                    else
                            echo "  don't use -p or remove manually."
                    fi
                else
                    echo "  $chksum_type ok. done!"
                    echo "$file" $(get_mod_time_ "$file") $chksum >> $CACHE_FILE
                fi
            fi
            #done!
            break
        done
        
        if ((failed)); then
            echo "download failed"
            # most common failure is certificate expiration, so check this
            #if we have the pasword we can retrigger download
            ((!skip_security)) && [[ "$pass" ]] && check_cert
            unset failed
        fi
        
    done <<<"$download_files"

}

dedup_cache_() {
    local file=${1:-${CACHE_FILE}}
    ((debug)) && echo "dedup'ing cache ${file} ..."
    local tmp=$(LC_ALL='C' sort  -r -k1,2 $file | awk '!($1 in a) {a[$1];print $0}' | sort -k2,2)
    ((DEBUG)) && echo "$tmp"
    echo "$tmp" > $file
    ((debug)) && echo "(cache dedup'ed)"
}

#do we have old results? Create the file if not
[ ! -f $CACHE_FILE ] && echo "#filename mtime checksum" > $CACHE_FILE && chmod 666 $CACHE_FILE

#
# MAIN
#

echo "Running $(basename $0) version: $version"
((verbose)) && echo "we use other tools in here, don't try to user their proposed 'options' directly"
echo "Use $(basename $0) -h for help."$'\n'

cat <<'EOF-MESSAGE'
Script created for {{ files|length }} file(s)
(The count won't match if you manually edit this file!)

EOF-MESSAGE
sleep 1

download

dedup_cache_

echo "done"
