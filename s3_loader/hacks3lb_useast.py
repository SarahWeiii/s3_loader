import socket
import random


lb = [
    "ceph00.nrp.hpc.udel.edu",
    "epic001.clemson.edu",
    "gpn-cisco1-nautilus.greatplains.net",
    "gpn-cisco2-nautilus.greatplains.net",
    "hcc-nrp-shor-c6025.unl.edu",
    "hcc-prp-c1312.unl.edu",
    "hcc-prp-c5027.unl.edu",
    "k8s-ceph-01.ultralight.org",
    "k8s-gen4-01.ampath.net",
    "k8s-igrok-01.calit2.optiputer.net",
    "k8s-usra-02.calit2.optiputer.net",
    "knuron.calit2.optiputer.net",
    "nrp-s1.nysernet.org",
    "osg.chic.nrp.internet2.edu",
    "osg.newy32aoa.nrp.internet2.edu"
]
w = [
    1 / 621.92,
    1 / 577.53,
    1 / 444.06,
    1 / 474.65,
    1 / 433.89,
    1 / 530.07,
    1 / 473.18,
    1 / 721.08,
    1 / 515.51,
    1 / 1109.04,
    1 / 643.26,
    1 / 616.65,
    1 / 482.12,
    1 / 718.72,
    1 / 511.23
]

x = 0

# Inspired by: https://stackoverflow.com/a/15065711/868533
# print(socket.getaddrinfo)
prv_getaddrinfo = socket.getaddrinfo
def new_getaddrinfo(*args):
    global x
    # Uncomment to see what calls to `getaddrinfo` look like.
    # print(args)
    if args[0] == 's3-haosu.nrp-nautilus.io':
        ingress = random.choices(lb, w, k=1)[0]
        res = [(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM, 6, '', (ingress, args[1]))]
        x += 1
        # print("-> HIJACK", res)
    else:
        res = prv_getaddrinfo(*args)
        # print("-> ORIG", res)
    return res

socket.getaddrinfo = new_getaddrinfo
